package ads

import (
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/oschwald/geoip2-golang"
)

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
	if p.reader == nil {
		return "", fmt.Errorf("geoip provider closed")
	}

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
	if p.reader == nil {
		return false, fmt.Errorf("geoip provider closed")
	}

	record, err := p.reader.AnonymousIP(ip)
	if err != nil {
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
	return "US", nil // Default mock
}

func (p *MockGeoProvider) IsAnonymous(ip string) (bool, error) {
	return strings.HasSuffix(ip, ".66") || strings.HasSuffix(ip, ".77"), nil
}

func (p *MockGeoProvider) Close() error { return nil }
