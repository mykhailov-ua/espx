package ads

import (
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/oschwald/maxminddb-golang"
)

type GeoProvider interface {
	GetCountry(ip string) (string, error)
	IsAnonymous(ip string) (bool, error)
	Close() error
}

type countryResult struct {
	Country struct {
		IsoCode string `maxminddb:"iso_code"`
	} `maxminddb:"country"`
}

type anonymousIPResult struct {
	IsAnonymous       bool `maxminddb:"is_anonymous"`
	IsAnonymousVPN    bool `maxminddb:"is_anonymous_vpn"`
	IsHostingProvider bool `maxminddb:"is_hosting_provider"`
	IsPublicProxy     bool `maxminddb:"is_public_proxy"`
	IsTorExitNode     bool `maxminddb:"is_tor_exit_node"`
}

var countryPool = sync.Pool{
	New: func() any {
		return &countryResult{}
	},
}

var anonymousIPPool = sync.Pool{
	New: func() any {
		return &anonymousIPResult{}
	},
}

// Reader is swapped under mu; lookups hold RLock for the duration of Lookup.
type MaxMindProvider struct {
	reader *maxminddb.Reader
	mu     sync.RWMutex
}

func NewMaxMindProvider(dbPath string) (*MaxMindProvider, error) {
	db, err := maxminddb.Open(dbPath)
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
	reader := p.reader
	p.mu.RUnlock()
	if reader == nil {
		return "", fmt.Errorf("geoip provider closed")
	}

	record := countryPool.Get().(*countryResult)
	record.Country.IsoCode = ""
	defer countryPool.Put(record)

	if err := reader.Lookup(ip, record); err != nil {
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
	reader := p.reader
	p.mu.RUnlock()
	if reader == nil {
		return false, fmt.Errorf("geoip provider closed")
	}

	record := anonymousIPPool.Get().(*anonymousIPResult)
	record.IsAnonymous = false
	record.IsAnonymousVPN = false
	record.IsHostingProvider = false
	record.IsPublicProxy = false
	record.IsTorExitNode = false
	defer anonymousIPPool.Put(record)

	if err := reader.Lookup(ip, record); err != nil {
		return false, err
	}

	return record.IsAnonymous || record.IsAnonymousVPN || record.IsHostingProvider || record.IsPublicProxy || record.IsTorExitNode, nil
}

func (p *MaxMindProvider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.reader != nil {
		err := p.reader.Close()
		p.reader = nil
		return err
	}
	return nil
}

type MockGeoProvider struct {
	Countries map[string]string
}

func (p *MockGeoProvider) GetCountry(ip string) (string, error) {
	if code, ok := p.Countries[ip]; ok {
		return code, nil
	}
	return "US", nil
}

func (p *MockGeoProvider) IsAnonymous(ip string) (bool, error) {
	return strings.HasSuffix(ip, ".66") || strings.HasSuffix(ip, ".77"), nil
}

func (p *MockGeoProvider) Close() error { return nil }
