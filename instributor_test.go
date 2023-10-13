package instributor_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/go-redis/redis/v8"
	"github.com/michalkurzeja/go-clock"
	"github.com/stretchr/testify/require"

	"github.com/michalkurzeja/instributor"
)

func TestInstributor(t *testing.T) {
	ctx := context.Background()

	t.Run("can acquire, refresh and release a key", func(t *testing.T) {
		t.Parallel()

		r, _ := newRedis(t)
		inst := instributor.New(r, "test", instributor.DefaultConfig())

		key, err := inst.Acquire(ctx)
		require.NoError(t, err)

		err = inst.Refresh(ctx, key)
		require.NoError(t, err)

		err = inst.Release(ctx, key)
		require.NoError(t, err)
	})
	t.Run("refreshing a key that does not exist does not add any set members", func(t *testing.T) {
		t.Parallel()

		r, mr := newRedis(t)
		inst := instributor.New(r, "test", instributor.DefaultConfig())

		err := inst.Refresh(ctx, "does-not-exist")
		require.NoError(t, err)

		_, err = mr.ZMembers("test:pool")
		require.EqualError(t, err, "ERR no such key")
	})
	t.Run("releasing a key that does not exist does not add any set members", func(t *testing.T) {
		t.Parallel()

		r, mr := newRedis(t)
		inst := instributor.New(r, "test", instributor.DefaultConfig())

		err := inst.Release(ctx, "does-not-exist")
		require.NoError(t, err)

		_, err = mr.ZMembers("test:pool")
		require.EqualError(t, err, "ERR no such key")
	})
	t.Run("acquiring a key after releasing it re-uses it", func(t *testing.T) {
		t.Parallel()

		r, _ := newRedis(t)
		inst := instributor.New(r, "test", instributor.DefaultConfig())

		firstKey, err := inst.Acquire(ctx)
		require.NoError(t, err)

		err = inst.Release(ctx, firstKey)
		require.NoError(t, err)

		for i := 0; i < 10; i++ {
			key, err := inst.Acquire(ctx)
			require.NoError(t, err)
			require.Equal(t, firstKey, key)

			err = inst.Release(ctx, key)
			require.NoError(t, err)
		}
	})
	t.Run("acquire prefers the most recently released key", func(t *testing.T) {
		t.Parallel()

		r, _ := newRedis(t)
		inst := instributor.New(r, "test", instributor.DefaultConfig())

		// Acquire 3 keys.
		key1, err := inst.Acquire(ctx)
		require.NoError(t, err)
		key2, err := inst.Acquire(ctx)
		require.NoError(t, err)
		key3, err := inst.Acquire(ctx)
		require.NoError(t, err)

		// Release them all in a different order.
		err = inst.Release(ctx, key1)
		require.NoError(t, err)
		err = inst.Release(ctx, key3)
		require.NoError(t, err)
		err = inst.Release(ctx, key2)
		require.NoError(t, err)

		// Now re-acquire 3 keys. We expect all 3 keys to be re-used and
		// returned in the reversed order to which they were released.
		keyReusedA, err := inst.Acquire(ctx)
		require.NoError(t, err)
		require.Equal(t, key2, keyReusedA)
		keyReusedB, err := inst.Acquire(ctx)
		require.NoError(t, err)
		require.Equal(t, key3, keyReusedB)
		keyReusedC, err := inst.Acquire(ctx)
		require.NoError(t, err)
		require.Equal(t, key1, keyReusedC)
	})
	t.Run("non-released key is automatically released after specified time", func(t *testing.T) {
		const acquisitionDuration = time.Second

		clk := clock.Mock(time.Now())
		t.Cleanup(clock.Restore)

		r, mr := newRedis(t)
		mr.SetTime(clk.Now())
		conf := instributor.DefaultConfig()
		conf.KeyAcquiredFor = acquisitionDuration
		inst := instributor.New(r, "test", conf)

		key, err := inst.Acquire(ctx)
		require.NoError(t, err)

		clk.Add(acquisitionDuration)
		mr.FastForward(acquisitionDuration)

		key2, err := inst.Acquire(ctx)
		require.NoError(t, err)

		require.Equal(t, key, key2, "expected the key to be reused")
	})
	t.Run("key can be refreshed to hold it for longer than the default acquisition time", func(t *testing.T) {
		const acquisitionDurationHalf = time.Second

		clk := clock.Mock(time.Now())
		t.Cleanup(clock.Restore)

		r, mr := newRedis(t)
		mr.SetTime(clk.Now())
		conf := instributor.DefaultConfig()
		conf.KeyAcquiredFor = 2 * acquisitionDurationHalf
		inst := instributor.New(r, "test", conf)

		key, err := inst.Acquire(ctx)
		require.NoError(t, err)

		clk.Add(acquisitionDurationHalf)
		mr.FastForward(acquisitionDurationHalf)

		err = inst.Refresh(ctx, key)
		require.NoError(t, err)

		clk.Add(acquisitionDurationHalf)
		mr.FastForward(acquisitionDurationHalf)

		key2, err := inst.Acquire(ctx) // Should get a new key because previous one is still acquired.
		require.NoError(t, err)

		require.NotEqual(t, key, key2, "expected a new key")
	})
	t.Run("lock contention test", func(t *testing.T) {
		t.Parallel()
		if testing.Short() {
			t.Skip("skipping test in short mode")
		}

		const tasks, loops = 100, 100
		r, mr := newRedis(t)
		inst := instributor.New(r, "test", instributor.DefaultConfig())

		var wg sync.WaitGroup
		wg.Add(tasks)
		for i := 0; i < tasks; i++ {
			go func() {
				defer wg.Done()
				for j := 0; j < loops; j++ {
					key, err := inst.Acquire(ctx)
					require.NoError(t, err)

					err = inst.Refresh(ctx, key)
					require.NoError(t, err)

					err = inst.Release(ctx, key)
					require.NoError(t, err)
				}
			}()
		}
		wg.Wait()

		members, err := mr.ZMembers("test:pool")
		require.NoError(t, err)
		require.LessOrEqual(t, len(members), tasks)
	})
}

func newRedis(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	t.Helper()

	mr := miniredis.RunT(t)
	return redis.NewClient(&redis.Options{Addr: mr.Addr()}), mr
}
