package instributor_test

import (
	"context"
	"testing"
	"time"

	"github.com/michalkurzeja/go-clock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/michalkurzeja/instributor"
	"github.com/michalkurzeja/instributor/mocks"
)

func TestRunner(t *testing.T) {
	ctx := context.Background()
	anyCtx := mock.MatchedBy(func(context.Context) bool { return true })

	t.Run("can start and stop, providing a key in between", func(t *testing.T) {
		t.Parallel()

		inst := mocks.NewInstributor(t)
		inst.EXPECT().Acquire(anyCtx).Return("key", nil)
		inst.EXPECT().Release(anyCtx, "key").Return(nil)
		runner := instributor.NewRunner(inst, instributor.DefaultRunnerConfig())

		var grp errgroup.Group
		grp.Go(func() error {
			return runner.Start(ctx)
		})

		time.Sleep(10 * time.Millisecond)
		require.Equal(t, "key", runner.Key())

		runner.Stop()
		require.NoError(t, grp.Wait())

		require.Empty(t, runner.Key())
	})
	t.Run("refreshes the key periodically", func(t *testing.T) {
		const acquisitionDurationHalf = time.Second

		clk := clock.Mock(time.Now())
		t.Cleanup(clock.Restore)

		inst := mocks.NewInstributor(t)
		inst.EXPECT().Acquire(anyCtx).Return("key", nil)
		inst.EXPECT().Refresh(anyCtx, "key").Return(nil)
		inst.EXPECT().Release(anyCtx, "key").Return(nil)
		conf := instributor.DefaultRunnerConfig()
		conf.RefreshInterval = acquisitionDurationHalf
		runner := instributor.NewRunner(inst, conf)

		var grp errgroup.Group
		grp.Go(func() error {
			return runner.Start(ctx)
		})

		time.Sleep(10 * time.Millisecond)

		for i := 0; i < 5; i++ {
			clk.Add(acquisitionDurationHalf)
			require.Equal(t, "key", runner.Key(), "expected the same key as before")
		}

		runner.Stop()
		require.NoError(t, grp.Wait())
	})
	t.Run("start returns error on acquire error", func(t *testing.T) {
		t.Parallel()

		inst := mocks.NewInstributor(t)
		inst.EXPECT().Acquire(anyCtx).Return("", assert.AnError)
		runner := instributor.NewRunner(inst, instributor.DefaultRunnerConfig())

		err := runner.Start(ctx)

		require.ErrorIs(t, err, assert.AnError)
	})
	t.Run("start returns error on release error", func(t *testing.T) {
		t.Parallel()

		inst := mocks.NewInstributor(t)
		inst.EXPECT().Acquire(anyCtx).Return("key", nil)
		inst.EXPECT().Release(anyCtx, "key").Return(assert.AnError)
		runner := instributor.NewRunner(inst, instributor.DefaultRunnerConfig())

		var grp errgroup.Group
		grp.Go(func() error {
			return runner.Start(ctx)
		})

		time.Sleep(10 * time.Millisecond)
		runner.Stop()

		require.ErrorIs(t, grp.Wait(), assert.AnError)
	})
	t.Run("start returns error on refresh error", func(t *testing.T) {
		const acquisitionDurationHalf = time.Second

		clk := clock.Mock(time.Now())
		t.Cleanup(clock.Restore)

		inst := mocks.NewInstributor(t)
		inst.EXPECT().Acquire(anyCtx).Return("key", nil)
		inst.EXPECT().Refresh(anyCtx, "key").Return(assert.AnError)
		conf := instributor.DefaultRunnerConfig()
		conf.RefreshInterval = acquisitionDurationHalf
		runner := instributor.NewRunner(inst, conf)

		var grp errgroup.Group
		grp.Go(func() error {
			return runner.Start(ctx)
		})

		time.Sleep(10 * time.Millisecond)
		clk.Add(acquisitionDurationHalf)

		require.ErrorIs(t, grp.Wait(), assert.AnError)
	})
}
