package ads

import (
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/oschwald/geoip2-golang"
)

// GeoProvider abstracts country-level IP lookups.
// Chosen to isolate 3rd-party geo-data dependencies from business logic.
type GeoProvider interface {
	GetCountry(ip string) (string, error)
	IsAnonymous(ip string) (bool, error)
	Close() error
}

type MaxMindProvider struct {
	reader *geoip2.Reader
	mu     sync.RWMutex
}

func NewMaxMindProvider(dbPath string) (*MaxMindProvider, error) {
	db, err := geoip2.Open(dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open maxmind db: %w", err)
	}
	return &MaxMindProvider{reader: db}, nil
}

func (p *MaxMindProvider) GetCountry(ipStr string) (string, error) {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return "", fmt.Errorf("invalid IP: %s", ipStr)
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	record, err := p.reader.Country(ip)
	if err != nil {
		return "", err
	}

	return record.Country.IsoCode, nil
}

func (p *MaxMindProvider) IsAnonymous(ipStr string) (bool, error) {
	ip := net.ParseIP(ipStr)
	if ip == nil {
		return false, fmt.Errorf("invalid IP: %s", ipStr)
	}

	p.mu.RLock()
	defer p.mu.RUnlock()

	// MaxMind Anonymous IP database provides this.
	// If the user is using the standard Country/City DB, this might not be available.
	// We'll try to cast to appropriate method if available or use a heuristic.
	// For now, let's assume we have the Anonymous IP DB or a combined one.
	record, err := p.reader.AnonymousIP(ip)
	if err != nil {
		return false, err
	}

	return record.IsAnonymous || record.IsAnonymousVPN || record.IsHostingProvider || record.IsPublicProxy || record.IsTorExitNode, nil
}

func (p *MaxMindProvider) Close() error {
	return p.reader.Close()
}

// MockGeoProvider used for testing or when DB is not available.
type MockGeoProvider struct {
	Countries map[string]string
}

func (p *MockGeoProvider) GetCountry(ip string) (string, error) {
	if code, ok := p.Countries[ip]; ok {
		return code, nil
	}
	return "US", nil // Default mock
}

func (p *MockGeoProvider) IsAnonymous(ip string) (bool, error) {
	// Heuristic: common cloud IPs for testing
	if strings.HasPrefix(ip, "10.0.") || strings.HasPrefix(ip, "192.168.") {
		return false, nil
	}
	// Assume some IPs are bots for testing
	return strings.HasSuffix(ip, ".66") || strings.HasSuffix(ip, ".77"), nil
}

func (p *MockGeoProvider) Close() error { return nil }
