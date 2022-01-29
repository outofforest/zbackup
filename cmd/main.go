package main

import (
	// nolint
	"crypto"
	"crypto/rand"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"
	"syscall"

	"github.com/go-piv/piv-go/piv"
	"github.com/outofforest/run"
	"golang.org/x/term"
)

const zfsKeyFile = "/usr/share/zfs-tools/zfs-pass.yubi"

var yubiSlot = piv.SlotAuthentication

func main() {
	run.Tool("zbackup", nil, func() error {
		pass, err := decryptPassword()
		if err != nil {
			return err
		}
		fmt.Println(pass)
		return nil
	})
}

func decryptPassword() (string, error) {
	cards, err := piv.Cards()
	if err != nil {
		return "", fmt.Errorf("fetching YubiKey devices failed: %w", err)
	}
	for _, ykCard := range cards {
		// inline function to close yubikey device immediately
		pass, err := func() (pass string, retErr error) {
			if !strings.Contains(strings.ToLower(ykCard), "yubikey") {
				return "", nil
			}

			yk, err := piv.Open(ykCard)
			if err != nil {
				return "", fmt.Errorf("opening YubiKey device failed: %w", err)
			}
			defer func() {
				if err := yk.Close(); err != nil && retErr == nil {
					retErr = fmt.Errorf("closing YubiKey device failed: %w", err)
				}
			}()

			cert, err := yk.Certificate(yubiSlot)
			if err != nil {
				return "", fmt.Errorf("fetching certificate failed: %w", err)
			}

			fmt.Printf("Hello %s, provide your YubiKey PIN:\n", cert.Subject.CommonName)

			pin, err := term.ReadPassword(syscall.Stdin)
			if err != nil {
				return "", fmt.Errorf("reading pin failed: %w", err)
			}
			pk, err := yk.PrivateKey(yubiSlot, cert.PublicKey, piv.KeyAuth{PIN: string(pin), PINPolicy: piv.PINPolicyAlways})
			if err != nil {
				return "", fmt.Errorf("fetching private key failed: %w", err)
			}

			privKey, ok := pk.(crypto.Decrypter)
			if !ok {
				return "", errors.New("private key stored on YubiKey can't be used for decryption")
			}

			passEncrypted, err := ioutil.ReadFile(zfsKeyFile)
			if err != nil {
				return "", fmt.Errorf("reading encrypted password failed: %w", err)
			}
			passDecrypted, err := privKey.Decrypt(rand.Reader, passEncrypted, nil)
			if err != nil {
				return "", fmt.Errorf("decryption failed: %w", err)
			}
			return string(passDecrypted), nil
		}()
		if err != nil {
			return "", err
		}
		if pass != "" {
			return pass, nil
		}
	}
	return "", errors.New("no YubiKey device has been detected")
}
