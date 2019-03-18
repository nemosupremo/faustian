package faustian

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/franela/goreq"
	log "github.com/sirupsen/logrus"
)

type VaultSecurityError error
type VaultServerError error

func NewVaultError(statusCode int, r io.Reader) error {
	var e VaultError
	e.Code = statusCode
	if err := json.NewDecoder(r).Decode(&e); err == nil {
		return e
	} else {
		e.Errors = []string{"communication error."}
		return e
	}
}

type VaultError struct {
	Code   int      `json:"-"`
	Errors []string `json:"errors"`
}

func (e VaultError) Error() string {
	return fmt.Sprintf("%d: %s", e.Code, strings.Join(e.Errors, ", "))
}

func vaultPath(addr string, path string) string {
	if u, err := url.Parse(addr); err == nil {
		u.Path = path
		return u.String()
	} else {
		panic(err)
	}
}

func gatekeeperToken(gatekeeperAddr, vaultAddr, taskId string) (string, error) {
	r, err := goreq.Request{
		Uri:         vaultPath(gatekeeperAddr, "/token"),
		Method:      "POST",
		ContentType: "application/json",
		Body: struct {
			TaskId string `json:"task_id"`
		}{taskId},
	}.Do()
	if err == nil {
		defer r.Body.Close()
		if r.StatusCode == http.StatusOK {
			var t struct {
				Token string `json:"token"`
			}
			if err := r.Body.FromJsonTo(&t); err == nil {
				r, err := goreq.Request{
					Uri:             vaultPath(vaultAddr, "/v1/sys/wrapping/unwrap"),
					Method:          "POST",
					MaxRedirects:    10,
					RedirectHeaders: true,
				}.WithHeader("X-Vault-Token", t.Token).Do()
				if err == nil {
					var t struct {
						Auth struct {
							ClientToken string `json:"client_token"`
						} `json:"auth"`
					}
					if r.StatusCode == http.StatusOK {
						if err := r.Body.FromJsonTo(&t); err == nil {
							return t.Auth.ClientToken, nil
						} else {
							return "", err
						}
					} else {
						return "", NewVaultError(r.StatusCode, r.Body)
					}
				} else {
					return "", err
				}
			} else {
				return "", VaultServerError(err)
			}
		} else if r.StatusCode == 429 {
			return "", VaultSecurityError(errors.New("Token already requested for this task."))
		} else {
			return "", VaultServerError(fmt.Errorf("Code %d when requesting token from Gatekeeper", r.StatusCode))
		}
	} else {
		return "", err
	}
}

type vaultToken struct {
	Addr  string
	Token string
}

func (t *vaultToken) TokenTtl() (time.Duration, error) {
	r, err := goreq.Request{
		Uri:             vaultPath(t.Addr, "/v1/auth/token/lookup-self"),
		MaxRedirects:    10,
		RedirectHeaders: true,
		Method:          "GET",
	}.WithHeader("X-Vault-Token", t.Token).Do()
	if err == nil {
		switch r.StatusCode {
		case 200:
			var resp struct {
				Data struct {
					Ttl int `json:"ttl"`
				} `json:"data"`
			}
			if err := r.Body.FromJsonTo(&resp); err == nil {
				return time.Duration(resp.Data.Ttl) * time.Second, nil
			} else {
				return 0, err
			}
		default:
			return 0, NewVaultError(r.StatusCode, r.Body)
		}
	} else {
		return 0, err
	}
}

func (t *vaultToken) RenewToken() error {
	r, err := goreq.Request{
		Uri:             vaultPath(t.Addr, "v1/auth/token/renew-self"),
		MaxRedirects:    10,
		RedirectHeaders: true,
		Method:          "POST",
	}.WithHeader("X-Vault-Token", t.Token).Do()
	if err == nil {
		switch r.StatusCode {
		case 200, 204:
			return nil
		default:
			return NewVaultError(r.StatusCode, r.Body)
		}
	} else {
		return err
	}
}

func (t *vaultToken) RenewalWorker(controlChan chan struct{}) {
	timer := time.NewTimer(1 * time.Hour)
	readTimer := false
	for {
		if ttl, err := t.TokenTtl(); err == nil {
			if ttl == 0 {
				// root token
				return
			}
			// refresh jitter so all api instances don't die simultaneously
			waitTime := ttl - (10 * time.Second)
			if waitTime > 10*time.Minute {
				minutes := time.Duration(rand.Intn(7)) * time.Minute
				waitTime -= minutes
			}
			if waitTime < 0 {
				waitTime = 0
			}
			if !timer.Stop() && !readTimer {
				<-timer.C
			}
			readTimer = false
			timer.Reset(waitTime)
			select {
			case <-timer.C:
				readTimer = true
			case <-controlChan:
				return
			}
			if err := t.RenewToken(); err == nil {
				log.Debugf("Renewed Vault Token (original ttl: %v)", ttl)
			} else {
				log.Fatalf("Failed to renew Vault token. Is the policy set correctly: %v", err)
				return
			}
		} else {
			log.Fatalf("Looking up our token's ttl caused an error: %v. Is the policy set correctly?", err)
			return
		}
	}
}

type vaultAwsCredProvider struct {
	Addr    string
	Token   string
	Expires time.Time
}

func NewVaultAwsCredProvider(addr, token string) credentials.Provider {
	return &vaultAwsCredProvider{
		Addr:  addr,
		Token: token,
	}
}

func (v *vaultAwsCredProvider) Retrieve() (credentials.Value, error) {
	if val, lease, err := vaultAwsCreds(v.Addr, v.Token); err == nil {
		v.Expires = time.Now().Add(lease).Add(-30 * time.Second)
		return val, nil
	} else {
		return credentials.Value{}, err
	}
}

func (v *vaultAwsCredProvider) IsExpired() bool {
	return time.Now().After(v.Expires)
}

func vaultAwsCreds(addr, token string) (credentials.Value, time.Duration, error) {
	r, err := goreq.Request{
		Uri:    vaultPath(addr, "/v1/aws/sts/api"),
		Method: "POST",
	}.WithHeader("X-Vault-Token", token).Do()
	if err == nil {
		defer r.Body.Close()
		if r.StatusCode == http.StatusOK {
			var resp struct {
				LeaseDuration int64 `json:"lease_duration"`
				Data          struct {
					AccessKey string `json:"access_key"`
					SecretKey string `json:"secret_key"`
					Token     string `json:"security_token"`
				} `json:"data"`
			}
			if err := r.Body.FromJsonTo(&resp); err == nil {
				return credentials.Value{
					AccessKeyID:     resp.Data.AccessKey,
					SecretAccessKey: resp.Data.SecretKey,
					SessionToken:    resp.Data.Token,
				}, (time.Second * time.Duration(resp.LeaseDuration)), nil
			} else {
				return credentials.Value{}, 0, err
			}
		} else {
			return credentials.Value{}, 0, NewVaultError(r.StatusCode, r.Body)
		}
	} else {
		return credentials.Value{}, 0, err
	}
}
