package syslog

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/grafana/dskit/backoff"
	"github.com/mwitkow/go-conntrack"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/grafana/loki/clients/pkg/promtail/scrapeconfig"
	"github.com/grafana/loki/clients/pkg/promtail/targets/syslog/syslogparser"
	"github.com/influxdata/go-syslog/v3"
	"github.com/prometheus/prometheus/model/labels"
)

var (
	protocolUDP = "udp"
	protocolTCP = "tcp"
)

type SyslogTransport interface {
	Run() error
	Addr() net.Addr
	Ready() bool
	Close() error
	Wait()
}

type handleMessage func(labels.Labels, syslog.Message)
type handleMessageError func(error)

type baseTransport struct {
	config *scrapeconfig.SyslogTargetConfig
	logger log.Logger

	openConnections *sync.WaitGroup

	handleMessage      handleMessage
	handleMessageError handleMessageError

	ctx       context.Context
	ctxCancel context.CancelFunc
}

func (t *baseTransport) close() {
	t.ctxCancel()
}

// Ready implements SyslogTransport
func (t *baseTransport) Ready() bool {
	return t.ctx.Err() == nil
}

func (t *baseTransport) idleTimeout() time.Duration {
	if t.config.IdleTimeout != 0 {
		return t.config.IdleTimeout
	}
	return defaultIdleTimeout
}

func (t *baseTransport) maxMessageLength() int {
	if t.config.MaxMessageLength != 0 {
		return t.config.MaxMessageLength
	}
	return defaultMaxMessageLength
}

func (t *baseTransport) connectionLabels(ip string) labels.Labels {
	lb := labels.NewBuilder(nil)
	for k, v := range t.config.Labels {
		lb.Set(string(k), string(v))
	}

	lb.Set("__syslog_connection_ip_address", ip)
	lb.Set("__syslog_connection_hostname", lookupAddr(ip))

	return lb.Labels()
}

func ipFromConn(c net.Conn) net.IP {
	switch addr := c.RemoteAddr().(type) {
	case *net.TCPAddr:
		return addr.IP
	}

	return nil
}

func lookupAddr(addr string) string {
	names, _ := net.LookupAddr(addr)
	return strings.Join(names, ",")
}

func newBaseTransport(config *scrapeconfig.SyslogTargetConfig, handleMessage handleMessage, handleError handleMessageError, logger log.Logger) *baseTransport {
	ctx, cancel := context.WithCancel(context.Background())
	return &baseTransport{
		config:             config,
		logger:             logger,
		openConnections:    new(sync.WaitGroup),
		handleMessage:      handleMessage,
		handleMessageError: handleError,
		ctx:                ctx,
		ctxCancel:          cancel,
	}
}

type idleTimeoutConn struct {
	net.Conn
	idleTimeout time.Duration
}

func (c *idleTimeoutConn) Write(p []byte) (int, error) {
	c.setDeadline()
	return c.Conn.Write(p)
}

func (c *idleTimeoutConn) Read(b []byte) (int, error) {
	c.setDeadline()
	return c.Conn.Read(b)
}

func (c *idleTimeoutConn) setDeadline() {
	_ = c.Conn.SetDeadline(time.Now().Add(c.idleTimeout))
}

type SyslogTCPTransport struct {
	*baseTransport
	listener net.Listener
}

func NewSyslogTCPTransport(config *scrapeconfig.SyslogTargetConfig, handleMessage handleMessage, handleError handleMessageError, logger log.Logger) SyslogTransport {
	return &SyslogTCPTransport{
		baseTransport: newBaseTransport(config, handleMessage, handleError, logger),
	}
}

// Run implements SyslogTransport
func (t *SyslogTCPTransport) Run() error {
	l, err := net.Listen(protocolTCP, t.config.ListenAddress)
	l = conntrack.NewListener(l, conntrack.TrackWithName("syslog_target/"+t.config.ListenAddress))
	if err != nil {
		return fmt.Errorf("error setting up syslog target: %w", err)
	}

	tlsEnabled := t.config.TLSConfig.CertFile != "" || t.config.TLSConfig.KeyFile != "" || t.config.TLSConfig.CAFile != ""
	if tlsEnabled {
		tlsConfig, err := newTLSConfig(t.config.TLSConfig.CertFile, t.config.TLSConfig.KeyFile, t.config.TLSConfig.CAFile)
		if err != nil {
			return fmt.Errorf("error setting up syslog target: %w", err)
		}
		l = tls.NewListener(l, tlsConfig)
	}

	t.listener = l
	level.Info(t.logger).Log("msg", "syslog listening on address", "address", t.Addr().String(), "protocol", protocolTCP, "tls", tlsEnabled)

	t.openConnections.Add(1)
	go t.acceptConnections()

	return nil
}

func newTLSConfig(certFile string, keyFile string, caFile string) (*tls.Config, error) {
	if certFile == "" || keyFile == "" {
		return nil, fmt.Errorf("certificate and key files are required")
	}

	certs, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("unable to load server certificate or key: %w", err)
	}

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{certs},
	}

	if caFile != "" {
		caCert, err := ioutil.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("unable to load client CA certificate: %w", err)
		}

		caCertPool := x509.NewCertPool()
		if ok := caCertPool.AppendCertsFromPEM(caCert); !ok {
			return nil, fmt.Errorf("unable to parse client CA certificate")
		}

		tlsConfig.ClientCAs = caCertPool
		tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
	}

	return tlsConfig, nil
}

func (t *SyslogTCPTransport) acceptConnections() {
	defer t.openConnections.Done()

	l := log.With(t.logger, "address", t.listener.Addr().String())

	backoff := backoff.New(t.ctx, backoff.Config{
		MinBackoff: 5 * time.Millisecond,
		MaxBackoff: 1 * time.Second,
	})

	for {
		c, err := t.listener.Accept()
		if err != nil {
			if !t.Ready() {
				level.Info(l).Log("msg", "syslog server shutting down")
				return
			}

			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				level.Warn(l).Log("msg", "failed to accept syslog connection", "err", err, "num_retries", backoff.NumRetries())
				backoff.Wait()
				continue
			}

			level.Error(l).Log("msg", "failed to accept syslog connection. quiting", "err", err)
			return
		}
		backoff.Reset()

		t.openConnections.Add(1)
		go t.handleConnection(c)
	}

}

func (t *SyslogTCPTransport) handleConnection(cn net.Conn) {
	defer t.openConnections.Done()

	c := &idleTimeoutConn{cn, t.idleTimeout()}

	handlerCtx, cancel := context.WithCancel(t.ctx)
	defer cancel()
	go func() {
		<-handlerCtx.Done()
		_ = c.Close()
	}()

	lbs := t.connectionLabels(ipFromConn(c).String())

	err := syslogparser.ParseStream(c, func(result *syslog.Result) {
		if err := result.Error; err != nil {
			t.handleMessageError(err)
			return
		}
		t.handleMessage(lbs.Copy(), result.Message)
	}, t.maxMessageLength())

	if err != nil {
		level.Warn(t.logger).Log("msg", "error initializing syslog stream", "err", err)
	}
}

// Close implements SyslogTransport
func (t *SyslogTCPTransport) Close() error {
	t.baseTransport.close()
	return t.listener.Close()
}

// Wait implements SyslogTransport
func (t *SyslogTCPTransport) Wait() {
	t.openConnections.Wait()
}

// Addr implements SyslogTransport
func (t *SyslogTCPTransport) Addr() net.Addr {
	return t.listener.Addr()
}

type SyslogUDPTransport struct {
	*baseTransport
	packetConn net.PacketConn
}

func NewSyslogUDPTransport(config *scrapeconfig.SyslogTargetConfig, handleMessage handleMessage, handleError handleMessageError, logger log.Logger) SyslogTransport {
	return &SyslogUDPTransport{
		baseTransport: newBaseTransport(config, handleMessage, handleError, logger),
	}
}

// Run implements SyslogTransport
func (t *SyslogUDPTransport) Run() error {
	l, err := net.ListenPacket(protocolUDP, t.config.ListenAddress)
	if err != nil {
		return fmt.Errorf("error setting up syslog target: %w", err)
	}
	t.packetConn = l
	level.Info(t.logger).Log("msg", "syslog listening on address", "address", t.Addr().String(), "protocol", protocolUDP)
	go t.acceptPackets()
	return nil
}

// Close implements SyslogTransport
func (t *SyslogUDPTransport) Close() error {
	t.baseTransport.close()
	return t.packetConn.Close()
}

func (t *SyslogUDPTransport) acceptPackets() {
	var (
		n    int
		addr net.Addr
		err  error
	)
	buf := make([]byte, t.maxMessageLength())
	for {
		if !t.Ready() {
			level.Info(t.logger).Log("msg", "syslog server shutting down")
			return
		}
		n, addr, err = t.packetConn.ReadFrom(buf)
		if err != nil {
			level.Info(t.logger).Log("msg", "failed to read packets", "addr", addr, "n", n, "err", err)
			continue
		}

		// TODO: is there a better way to pass a copy to the handleRcv() function?
		b := make([]byte, n)
		copy(b, buf[:n])
		t.openConnections.Add(1)
		go t.handleRcv(addr, b)
	}
}

func (t *SyslogUDPTransport) handleRcv(addr net.Addr, buf []byte) {
	defer t.openConnections.Done()

	lbs := t.connectionLabels(addr.String())

	err := syslogparser.ParseStream(bytes.NewReader(buf), func(result *syslog.Result) {
		if err := result.Error; err != nil {
			t.handleMessageError(err)
			return
		}
		t.handleMessage(lbs.Copy(), result.Message)
	}, t.maxMessageLength())

	if err != nil {
		level.Warn(t.logger).Log("msg", "error parsing syslog stream", "err", err)
	}
}

// Wait implements SyslogTransport
func (t *SyslogUDPTransport) Wait() {
	t.openConnections.Wait()
}

// Addr implements SyslogTransport
func (t *SyslogUDPTransport) Addr() net.Addr {
	return t.packetConn.LocalAddr()
}
