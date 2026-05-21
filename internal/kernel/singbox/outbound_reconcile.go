package singbox

import (
	"bytes"
	"context"
	"fmt"
	"strings"

	"github.com/sagernet/sing-box/adapter"
	singLog "github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	singJSON "github.com/sagernet/sing/common/json"

	"github.com/cedar2025/xboard-node/internal/nlog"
)

// outboundReconcile diffs old vs new outbound option lists and applies the
// changes against the OutboundManager so that hot-reload picks up additions,
// modifications and removals. The execution order is:
//
//  1. Create added/changed outbounds (in dependency order — deps first).
//  2. Run updateRulesFn (the caller's router.UpdateRules) so new rules can
//     resolve the freshly-installed tags.
//  3. Remove deleted outbounds (in reverse-dependency order — dependants first).
//
// Manager.Create handles the "tag already exists" case internally by closing
// the previous instance and swapping in the new one, so we always call Create
// for additions and modifications without an explicit pre-Remove.
func outboundReconcile(
	ctx context.Context,
	om adapter.OutboundManager,
	router adapter.Router,
	logFactory singLog.Factory,
	oldOuts, newOuts []option.Outbound,
	updateRulesFn func() error,
) error {
	oldByTag, err := indexOutboundsByTag(ctx, oldOuts)
	if err != nil {
		return fmt.Errorf("marshal previous outbounds: %w", err)
	}
	newByTag, err := indexOutboundsByTag(ctx, newOuts)
	if err != nil {
		return fmt.Errorf("marshal new outbounds: %w", err)
	}

	var toCreate []*option.Outbound
	for tag, nb := range newByTag {
		if ob, ok := oldByTag[tag]; ok && bytes.Equal(ob.json, nb.json) {
			continue
		}
		toCreate = append(toCreate, nb.ob)
	}

	var toRemove []string
	for tag := range oldByTag {
		if _, ok := newByTag[tag]; !ok {
			toRemove = append(toRemove, tag)
		}
	}

	sortedCreate, err := topoSortByDependencies(toCreate)
	if err != nil {
		return err
	}

	for _, ob := range sortedCreate {
		name := fmt.Sprintf("outbound/%s[%s]", ob.Type, ob.Tag)
		logger := logFactory.NewLogger(name)
		if err := om.Create(ctx, router, logger, ob.Tag, ob.Type, ob.Options); err != nil {
			return fmt.Errorf("create outbound %s: %w", ob.Tag, err)
		}
		nlog.Core().Debug("outbound created/replaced", "tag", ob.Tag, "type", ob.Type)
	}

	if updateRulesFn != nil {
		if err := updateRulesFn(); err != nil {
			return err
		}
	}

	if err := removeOutboundsInOrder(om, toRemove); err != nil {
		return err
	}

	return nil
}

type indexedOutbound struct {
	ob   *option.Outbound
	json []byte
}

// indexOutboundsByTag marshals each outbound to its canonical JSON form and
// indexes by tag. Untagged outbounds are skipped because we cannot diff them
// reliably across reloads (the manager itself rejects empty tags on Create).
func indexOutboundsByTag(ctx context.Context, outs []option.Outbound) (map[string]indexedOutbound, error) {
	res := make(map[string]indexedOutbound, len(outs))
	for i := range outs {
		ob := &outs[i]
		if ob.Tag == "" {
			continue
		}
		data, err := singJSON.MarshalContext(ctx, ob)
		if err != nil {
			return nil, fmt.Errorf("marshal outbound %s: %w", ob.Tag, err)
		}
		res[ob.Tag] = indexedOutbound{ob: ob, json: data}
	}
	return res, nil
}

// outboundDependencies extracts the tags this outbound depends on. Covers the
// common cases: DialerOptions.Detour for protocol outbounds, and the explicit
// Outbounds list on Selector / URLTest groups.
func outboundDependencies(ob *option.Outbound) []string {
	var deps []string
	if dw, ok := ob.Options.(option.DialerOptionsWrapper); ok {
		if d := dw.TakeDialerOptions().Detour; d != "" {
			deps = append(deps, d)
		}
	}
	switch o := ob.Options.(type) {
	case *option.SelectorOutboundOptions:
		deps = append(deps, o.Outbounds...)
	case *option.URLTestOutboundOptions:
		deps = append(deps, o.Outbounds...)
	}
	return deps
}

// topoSortByDependencies orders outbounds so each comes after every tag it
// depends on within the slice. Tags referenced as dependencies that aren't in
// the input slice are assumed to already exist on the manager — they don't
// block ordering. Returns an error on a true dependency cycle inside the
// input.
func topoSortByDependencies(outs []*option.Outbound) ([]*option.Outbound, error) {
	tagSet := make(map[string]bool, len(outs))
	for _, ob := range outs {
		tagSet[ob.Tag] = true
	}

	placed := make(map[string]bool, len(outs))
	res := make([]*option.Outbound, 0, len(outs))
	for len(res) < len(outs) {
		progressed := false
		for _, ob := range outs {
			if placed[ob.Tag] {
				continue
			}
			ready := true
			for _, dep := range outboundDependencies(ob) {
				if tagSet[dep] && !placed[dep] {
					ready = false
					break
				}
			}
			if !ready {
				continue
			}
			placed[ob.Tag] = true
			res = append(res, ob)
			progressed = true
		}
		if !progressed {
			var stuck []string
			for _, ob := range outs {
				if !placed[ob.Tag] {
					stuck = append(stuck, ob.Tag)
				}
			}
			return nil, fmt.Errorf("outbound dependency cycle among %v", stuck)
		}
	}
	return res, nil
}

// removeOutboundsInOrder calls om.Remove repeatedly until every tag is gone.
// The OutboundManager rejects Remove on a tag that is still depended on, so we
// loop and retry: each pass should clear at least one tag (the most-dependant
// ones first). If a pass makes zero progress, something outside the diff is
// still holding the dependency — surface that as an error rather than spin.
func removeOutboundsInOrder(om adapter.OutboundManager, toRemove []string) error {
	pending := append([]string(nil), toRemove...)
	for len(pending) > 0 {
		progressed := false
		var stillBlocked []string
		for _, tag := range pending {
			if err := om.Remove(tag); err != nil {
				if strings.Contains(err.Error(), "is depended by") {
					stillBlocked = append(stillBlocked, tag)
					continue
				}
				// Non-dependency error (e.g. tag already removed) — log and move on.
				nlog.Core().Warn("outbound remove failed", "tag", tag, "error", err)
				progressed = true
				continue
			}
			progressed = true
			nlog.Core().Debug("outbound removed", "tag", tag)
		}
		if !progressed {
			return fmt.Errorf("cannot remove outbounds blocked by external dependents: %v", stillBlocked)
		}
		pending = stillBlocked
	}
	return nil
}
