package ingester

import "golang.org/x/net/context"

// StopIncomingRequests implements ring.Lifecycler.
func (i *Ingester) StopIncomingRequests() {
	i.instancesMtx.Lock()
	defer i.instancesMtx.Unlock()
	i.readonly = true
}

// TransferOut implements ring.Lifecycler.
func (i *Ingester) TransferOut(ctx context.Context) error {
	return nil
}
