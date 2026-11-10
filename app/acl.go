package main

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"sync"
)

type ACLUser struct {
	Username  string
	NoPass    bool
	Passwords []string
}

type ACLManager struct {
	mu    sync.RWMutex
	users map[string]*ACLUser
}

func NewACLManager() *ACLManager {
	mgr := &ACLManager{
		users: make(map[string]*ACLUser),
	}
	mgr.users["default"] = &ACLUser{
		Username:  "default",
		NoPass:    true,
		Passwords: make([]string, 0),
	}
	return mgr
}

var aclManager = NewACLManager()

func (m *ACLManager) GetUser(username string) (*ACLUser, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	user, exists := m.users[username]
	if !exists {
		return nil, false
	}
	uCopy := *user
	return &uCopy, true
}

func (m *ACLManager) SetUser(username string, rules []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	user, exists := m.users[username]
	if !exists {
		user = &ACLUser{
			Username:  username,
			NoPass:    false,
			Passwords: make([]string, 0),
		}
	}

	for _, rule := range rules {
		if strings.HasPrefix(rule, ">") {
			rawPass := rule[1:]
			hash := sha256.Sum256([]byte(rawPass))
			hashHex := fmt.Sprintf("%x", hash)

			user.NoPass = false
			user.Passwords = append(user.Passwords, hashHex)

		} else if strings.EqualFold(rule, "nopass") {
			user.NoPass = true
			user.Passwords = make([]string, 0)
		}
	}

	return nil
}

func (m *ACLManager) Authenticate(username, password string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	user, exists := m.users[username]
	if !exists {
		return false
	}

	if user.NoPass {
		return true
	}

	hash := sha256.Sum256([]byte(password))
	hashHex := fmt.Sprintf("%x", hash)

	for _, storedhash := range user.Passwords {
		if storedhash == hashHex {
			return true
		}
	}

	return false

}
