package service

import (
	"context"
	"errors"
	"testing"

	"github.com/cedar2025/xboard-node/internal/model"
)

// A node that starts before the panel has any users for it (deployed before its
// database was attached) must still start its kernel when the users finally
// arrive. Before the fix the update returned early because no users were
// recorded yet, and nothing ever recorded them — the node discarded a full user
// list every push interval forever.
func TestColdStart_UsersArrivingWhileKernelDownStartTheKernel(t *testing.T) {
	k := &fakeKernel{running: false}
	s := newTestService(k)
	s.lastConfig = &model.NodeSpec{Protocol: "vless"}
	// No users recorded yet — exactly the state after "no users, kernel will
	// not start until users are available".
	if len(s.lastUsers) != 0 {
		t.Fatalf("precondition: lastUsers = %d, want 0", len(s.lastUsers))
	}

	users := []model.UserSpec{{ID: 1, UUID: "u1"}, {ID: 2, UUID: "u2"}}
	s.applyUserUpdate(context.Background(), users, computeUserHash(users), "test")

	if !k.running {
		t.Fatal("内核仍未启动 —— 到达的用户被丢弃了")
	}
	if k.startCalls != 1 {
		t.Fatalf("Start 调用 %d 次，want 1", k.startCalls)
	}
	if len(s.lastUsers) != 2 {
		t.Fatalf("lastUsers = %d，want 2（用户必须被记录，否则下一轮又会走同一条路）", len(s.lastUsers))
	}
}

// Second push with the same users must not restart the kernel.
func TestColdStart_ConvergesAfterFirstPush(t *testing.T) {
	k := &fakeKernel{running: false}
	s := newTestService(k)
	s.lastConfig = &model.NodeSpec{Protocol: "vless"}
	users := []model.UserSpec{{ID: 1, UUID: "u1"}}

	s.applyUserUpdate(context.Background(), users, computeUserHash(users), "test")
	first := k.startCalls
	s.applyUserUpdate(context.Background(), users, computeUserHash(users), "test")

	if k.startCalls != first {
		t.Fatalf("第二次推送又启动了一次内核（%d → %d）", first, k.startCalls)
	}
	if !k.running {
		t.Fatal("内核不应被停掉")
	}
}

// Without a config there is nothing to start; users must not be applied.
func TestColdStart_NoConfigStillSkips(t *testing.T) {
	k := &fakeKernel{running: false}
	s := newTestService(k)
	s.lastConfig = nil

	users := []model.UserSpec{{ID: 1, UUID: "u1"}}
	s.applyUserUpdate(context.Background(), users, computeUserHash(users), "test")

	if k.startCalls != 0 {
		t.Fatalf("没有配置却尝试启动内核 %d 次", k.startCalls)
	}
	if k.running {
		t.Fatal("没有配置时内核不应运行")
	}
}

// A failed start must not leave the service claiming users it never applied.
func TestColdStart_FailedStartRollsBackUserState(t *testing.T) {
	k := &fakeKernel{running: false, startErr: errors.New("start failed")}
	s := newTestService(k)
	s.lastConfig = &model.NodeSpec{Protocol: "vless"}

	users := []model.UserSpec{{ID: 1, UUID: "u1"}}
	s.applyUserUpdate(context.Background(), users, computeUserHash(users), "test")

	if k.running {
		t.Fatal("Start 失败却认为内核在跑")
	}
	if len(s.lastUsers) != 0 {
		t.Fatalf("Start 失败后 lastUsers = %d，want 0（不能记录未应用的用户）", len(s.lastUsers))
	}
}

// An empty user list must not start the kernel — "no users" is a real state.
func TestColdStart_EmptyUserListDoesNotStart(t *testing.T) {
	k := &fakeKernel{running: false}
	s := newTestService(k)
	s.lastConfig = &model.NodeSpec{Protocol: "vless"}

	s.applyUserUpdate(context.Background(), []model.UserSpec{}, "", "test")

	if k.startCalls != 0 {
		t.Fatalf("空用户列表却启动了内核 %d 次", k.startCalls)
	}
}

// The kernel resolves a user's limiter once, when it accepts a connection, and
// keeps it for that connection's lifetime. So the limiter must already know the
// seeded users when the inbound opens — otherwise a connection accepted in the
// gap stays unlimited for hours.
func TestColdStart_LimiterIsPrimedBeforeInboundOpens(t *testing.T) {
	k := &fakeKernel{running: false}
	s := newTestService(k)
	s.lastConfig = &model.NodeSpec{Protocol: "vless"}

	var limiterKnownAtStart bool
	k.onStart = func(users []model.UserSpec) {
		// Called from inside kernel.Start, i.e. exactly when the inbound opens.
		limiterKnownAtStart = s.speedTracker.GetLimiter("u1") != nil
	}

	users := []model.UserSpec{{ID: 1, UUID: "u1", SpeedLimit: 10}}
	s.applyUserUpdate(context.Background(), users, computeUserHash(users), "test")

	if !k.running {
		t.Fatal("内核未启动")
	}
	if !limiterKnownAtStart {
		t.Fatal("内核开始监听时 limiter 还不认识这批用户 —— 此刻接入的连接会终生不限速")
	}
}
