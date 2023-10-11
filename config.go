package instributor

import (
	"time"
)

type Config struct {
	// AttemptToLockFor is the amount of time to spend attempting to acquire a lock.
	AttemptToLockFor time.Duration
	// LockTTL is the amount of time the lock is valid for.
	// It's a failsafe for when the lock is not released due to e.g. an application crash.
	LockTTL time.Duration

	// KeyAcquiredFor is the amount of time the key is acquired for. After that, the key is considered free again.
	// To hold the key for longer, it needs to be refreshed.
	KeyAcquiredFor time.Duration
	// KeyTTL is the amount of time the key remains in the pool. After that, it will be lazily removed.
	KeyTTL time.Duration
	// KeyLen is the number of characters in the key. The longer the key, the lower the chance for a conflict.
	KeyLen uint
}

func DefaultConfig() Config {
	return Config{
		AttemptToLockFor: 10 * time.Second,
		LockTTL:          10 * time.Second,

		KeyAcquiredFor: 30 * time.Second,
		KeyTTL:         30 * 24 * time.Hour,
		KeyLen:         6,
	}
}

type RunnerConfig struct {
	// RefreshInterval is the amount of time between key refreshes.
	// It is recommended to set it to at most half of KeyAcquiredFor value.
	RefreshInterval time.Duration
	// AttemptToRefreshFor is the amount of time to spend attempting to refresh the key.
	// If the operation takes longer than that, the runner will stop with an error.
	// This is a failsafe to prevent a long refresh from unknowingly "losing" the key due to timed release.
	AttemptToRefreshFor time.Duration
}

func DefaultRunnerConfig() RunnerConfig {
	return RunnerConfig{
		RefreshInterval:     15 * time.Second,
		AttemptToRefreshFor: 10 * time.Second,
	}
}
