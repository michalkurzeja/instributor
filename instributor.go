package instributor

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/michalkurzeja/go-clock"
)

type Instributor interface {
	Acquire(ctx context.Context) (string, error)
	Refresh(ctx context.Context, key string) error
	Release(ctx context.Context, key string) error
}

type instributor struct {
	redis   redis.UniversalClient
	queries queries

	conf Config
}

func New(redis redis.UniversalClient, name string, config Config) Instributor {
	return &instributor{
		redis: redis,
		queries: queries{
			poolKey: fmt.Sprintf("%s:pool", name),
			lockKey: fmt.Sprintf("%s:lock", name),
		},
		conf: config,
	}
}

func (i *instributor) Acquire(ctx context.Context) (string, error) {
	ctx, unlock, err := i.lock(ctx)
	if err != nil {
		return "", err
	}
	defer unlock()

	var key string

	// Even though we're locked, we should still watch the pool for changes to guarantee we're resistant to data races.
	// There is a very slight chance of running this code on an expired lock (if the key in Redis expired
	// but the context did not yet - due to mismatched system clocks).
	err = i.queries.watchPool(ctx, i.redis, func(tx *redis.Tx) error {
		key, err = i.queries.findReleasedKey(ctx, tx, i.releasedScore())
		if err != nil {
			return fmt.Errorf("failed to find released key: %v", err)
		}

		if key == "" {
			key = i.generateKey() // If we haven't found any released keys, generate a new one.
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			err := i.queries.upsertKey(ctx, pipe, key, i.acquiredScore())
			if err != nil {
				return fmt.Errorf("failed to upsert key: %v", err)
			}
			err = i.queries.expireKeys(ctx, pipe, i.expiredScore())
			if err != nil {
				return fmt.Errorf("failed to expire keys: %v", err)
			}
			return nil
		})
		return err
	})
	if err != nil {
		return "", fmt.Errorf("failed to acquire key: %v", err)
	}

	return key, nil
}

func (i *instributor) Refresh(ctx context.Context, key string) error {
	ctx, unlock, err := i.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()

	err = i.queries.updateKey(ctx, i.redis, key, i.acquiredScore())
	if err != nil {
		return fmt.Errorf("failed to refresh key: %v", err)
	}
	return nil
}

func (i *instributor) Release(ctx context.Context, key string) error {
	ctx, unlock, err := i.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()

	err = i.queries.updateKey(ctx, i.redis, key, i.releasedScore())
	if err != nil {
		return fmt.Errorf("failed to release key: %v", err)
	}

	return nil
}

// lock attempts to acquire a lock before working on the pool.
// It returns a context with a deadline and a cleanup function.
// The attempt has a time limit after which it fails.
//
// Once the lock is acquired, it's valid for a limited amount of time. This function sets a deadline on context
// equal to the lock TTL to avoid working on the pool with an expired lock.
// This is just a precaution, with large enough TTLs lock expiry is quite unlikely to happen.
//
// The cleanup function unlocks the lock and cancels the context.
// It should be called in a defer statement right after locking.
func (i *instributor) lock(ctx context.Context) (timeoutCtx context.Context, unlock func(), err error) {
	timer := clock.Timer(i.conf.AttemptToLockFor)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, nil, fmt.Errorf("failed to lock: %w", ctx.Err())
		case <-timer.C:
			return nil, nil, errors.New("failed to lock: attempt timed out")
		default:
			// Start the timeout slightly before locking to have the context expired
			// and to prevent further operations BEFORE the lock is gone from redis.
			timeoutCtx, cancel := context.WithTimeout(ctx, i.conf.LockTTL)

			locked, err := i.queries.lock(ctx, i.redis, i.conf.LockTTL)
			if !locked {
				cancel() // Cancel the timeout. We'll start a new one in the next attempt.
				i.waitWithJitter(ctx, 10*time.Millisecond)
				continue
			}
			if err != nil {
				cancel() // Cancel the timeout. The lock is not acquired.
				return nil, nil, fmt.Errorf("failed to lock: %v", err)
			}

			return timeoutCtx, func() {
				_ = i.unlock(timeoutCtx)
				cancel()
			}, nil
		}
	}
}

func (i *instributor) waitWithJitter(ctx context.Context, d time.Duration) {
	jitter := time.Duration(rand.Intn(10000)) * time.Microsecond // 0-10ms
	timer := clock.Timer(d + jitter)
	defer timer.Stop()

	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func (i *instributor) unlock(ctx context.Context) error {
	err := i.queries.unlock(ctx, i.redis)
	if err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("failed to unlock: %v", err)
	}
	return nil
}

func (i *instributor) generateKey() string {
	return RandString(int(i.conf.KeyLen))
}

func (i *instributor) acquiredScore() int64 {
	return clock.Now().UnixNano()
}

func (i *instributor) releasedScore() int64 {
	return clock.Now().Add(-i.conf.KeyAcquiredFor).UnixNano()
}

func (i *instributor) expiredScore() int64 {
	return clock.Now().Add(-i.conf.KeyTTL).UnixNano()
}

type queries struct {
	poolKey string
	lockKey string
}

func (q queries) watchPool(ctx context.Context, r redis.UniversalClient, fn func(tx *redis.Tx) error) error {
	return r.Watch(ctx, fn, q.poolKey)
}

func (q queries) lock(ctx context.Context, r redis.Cmdable, ttl time.Duration) (bool, error) {
	err := r.SetArgs(ctx, q.lockKey, true, redis.SetArgs{Mode: "NX", TTL: ttl}).Err()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (q queries) unlock(ctx context.Context, r redis.Cmdable) error {
	return r.Del(ctx, q.lockKey).Err()
}

func (q queries) findReleasedKey(ctx context.Context, r redis.Cmdable, releasedScore int64) (string, error) {
	res, err := r.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key:     q.poolKey,
		Count:   1,
		ByScore: true,
		Rev:     true,
		Stop:    releasedScore,
	}).Result()
	if err != nil {
		return "", err
	}
	if len(res) == 0 {
		return "", nil
	}
	return res[0], nil
}

func (q queries) upsertKey(ctx context.Context, r redis.Cmdable, key string, acquiredScore int64) error {
	return r.ZAdd(ctx, q.poolKey, &redis.Z{Member: key, Score: float64(acquiredScore)}).Err()
}

func (q queries) updateKey(ctx context.Context, r redis.Cmdable, key string, score int64) error {
	return r.ZAddArgs(ctx, q.poolKey, redis.ZAddArgs{
		Members: []redis.Z{{Member: key, Score: float64(score)}},
		XX:      true, // Only update the element if it already exists.
	}).Err()
}

func (q queries) expireKeys(ctx context.Context, r redis.Cmdable, expiredScore int64) error {
	return r.ZRemRangeByScore(ctx, q.poolKey, "-inf", strconv.FormatInt(expiredScore, 10)).Err()
}
