package management

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"time"
)

type NginxConfigWorker struct {
	svc        *Service
	exportPath string
}

func NewNginxConfigWorker(svc *Service, exportPath string) *NginxConfigWorker {
	return &NginxConfigWorker{
		svc:        svc,
		exportPath: exportPath,
	}
}

func (w *NginxConfigWorker) Start(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := w.ExportAndReload(ctx); err != nil {
				slog.Error("nginx export failed", "error", err)
			}
		}
	}
}

func (w *NginxConfigWorker) ExportAndReload(ctx context.Context) error {
	if len(w.svc.rdbs) == 0 {
		return fmt.Errorf("no redis client available")
	}

	var manual []string
	for _, rdb := range w.svc.rdbs {
		m, err := rdb.SMembers(ctx, "blacklist:manual").Result()
		if err != nil {
			return fmt.Errorf("failed to fetch manual blacklist from shard: %w", err)
		}
		manual = append(manual, m...)
	}
	if err := w.writeDenyFile("manual.conf", manual); err != nil {
		return err
	}

	var auto []string
	for _, rdb := range w.svc.rdbs {
		a, err := rdb.SMembers(ctx, "blacklist:auto").Result()
		if err != nil {
			return fmt.Errorf("failed to fetch auto blacklist from shard: %w", err)
		}
		auto = append(auto, a...)
	}
	if err := w.writeDenyFile("auto.conf", auto); err != nil {
		return err
	}

	flagPath := filepath.Join(w.exportPath, "reload_required.flg")
	if err := os.WriteFile(flagPath, []byte("1\n"), 0644); err != nil {
		return fmt.Errorf("failed to write reload flag: %w", err)
	}

	slog.Info("nginx blacklist exported and reload signaled via flag file", "manual_count", len(manual), "auto_count", len(auto))
	return nil
}

func (w *NginxConfigWorker) writeDenyFile(filename string, ips []string) (err error) {
	if err := os.MkdirAll(w.exportPath, 0755); err != nil {
		return err
	}

	path := filepath.Join(w.exportPath, filename)
	tmpPath := path + ".tmp"

	tmpFile, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open temp config file: %w", err)
	}

	defer func() {
		if err != nil {
			_ = tmpFile.Close()
			_ = os.Remove(tmpPath)
		}
	}()

	bw := bufio.NewWriter(tmpFile)
	for _, ip := range ips {
		if ip == "" {
			continue
		}

		if net.ParseIP(ip) == nil {
			if _, _, errCIDR := net.ParseCIDR(ip); errCIDR != nil {
				slog.Warn("skipping invalid blacklist IP/CIDR to prevent injection", "ip", ip)
				continue
			}
		}

		if _, err = bw.WriteString("deny "); err != nil {
			return fmt.Errorf("failed to write directive prefix: %w", err)
		}
		if _, err = bw.WriteString(ip); err != nil {
			return fmt.Errorf("failed to write IP: %w", err)
		}
		if _, err = bw.WriteString(";\n"); err != nil {
			return fmt.Errorf("failed to write directive suffix: %w", err)
		}
	}

	if err = bw.Flush(); err != nil {
		return fmt.Errorf("failed to flush config buffer: %w", err)
	}

	if err = tmpFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync config file: %w", err)
	}

	if err = tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to close temp config file: %w", err)
	}

	if err = os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("failed to atomically replace config file: %w", err)
	}

	return nil
}
