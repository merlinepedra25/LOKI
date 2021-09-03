package dshttp

import "flag"

// BasicAuth configures basic authentication for HTTP clients.
type BasicAuth struct {
	Username string `yaml:"basic_auth_username"`
	Password string `yaml:"basic_auth_password"`
}

func (b *BasicAuth) RegisterFlagsWithPrefix(prefix string, f *flag.FlagSet) {
	f.StringVar(&b.Username, prefix+"basic-auth-username", "", "HTTP Basic authentication username. It overrides the username set in the URL (if any).")
	f.StringVar(&b.Password, prefix+"basic-auth-password", "", "HTTP Basic authentication password. It overrides the password set in the URL (if any).")
}

// IsEnabled returns false if basic authentication isn't enabled.
func (b BasicAuth) IsEnabled() bool {
	return b.Username != "" || b.Password != ""
}
