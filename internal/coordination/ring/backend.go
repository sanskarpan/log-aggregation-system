package ring

import (
	"context"
	"encoding/json"
	"sort"
	"strings"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type LeaseClient interface {
	Get(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error)
	Put(ctx context.Context, key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error)
	Delete(ctx context.Context, key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error)
	Grant(ctx context.Context, ttl int64) (*clientv3.LeaseGrantResponse, error)
	KeepAliveOnce(ctx context.Context, id clientv3.LeaseID) (*clientv3.LeaseKeepAliveResponse, error)
	Revoke(ctx context.Context, id clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error)
}

type Backend interface {
	PutMember(ctx context.Context, prefix string, member Member, ttl int64) (Member, error)
	DeleteMember(ctx context.Context, prefix, memberID string) error
	ListMembers(ctx context.Context, prefix string) ([]Member, error)
	RefreshLease(ctx context.Context, leaseID int64) error
	RevokeLease(ctx context.Context, leaseID int64) error
}

type EtcdBackend struct {
	client LeaseClient
}

func NewEtcdBackend(client LeaseClient) *EtcdBackend {
	return &EtcdBackend{client: client}
}

func (b *EtcdBackend) PutMember(ctx context.Context, prefix string, member Member, ttl int64) (Member, error) {
	var leaseID clientv3.LeaseID
	if member.LeaseID == 0 {
		lease, err := b.client.Grant(ctx, ttl)
		if err != nil {
			return Member{}, err
		}
		leaseID = lease.ID
		member.LeaseID = int64(lease.ID)
	} else {
		leaseID = clientv3.LeaseID(member.LeaseID)
	}
	member.UpdatedAt = member.UpdatedAt.UTC()
	encoded, err := json.Marshal(member)
	if err != nil {
		return Member{}, err
	}
	_, err = b.client.Put(ctx, memberKey(prefix, member.ID), string(encoded), clientv3.WithLease(leaseID))
	return member, err
}

func (b *EtcdBackend) DeleteMember(ctx context.Context, prefix, memberID string) error {
	_, err := b.client.Delete(ctx, memberKey(prefix, memberID))
	return err
}

func (b *EtcdBackend) ListMembers(ctx context.Context, prefix string) ([]Member, error) {
	resp, err := b.client.Get(ctx, memberPrefix(prefix), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	members := make([]Member, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var member Member
		if err := json.Unmarshal(kv.Value, &member); err != nil {
			return nil, err
		}
		members = append(members, member)
	}
	sort.Slice(members, func(i, j int) bool {
		return members[i].ID < members[j].ID
	})
	return members, nil
}

func (b *EtcdBackend) RefreshLease(ctx context.Context, leaseID int64) error {
	_, err := b.client.KeepAliveOnce(ctx, clientv3.LeaseID(leaseID))
	return err
}

func (b *EtcdBackend) RevokeLease(ctx context.Context, leaseID int64) error {
	_, err := b.client.Revoke(ctx, clientv3.LeaseID(leaseID))
	return err
}

func memberPrefix(prefix string) string {
	return strings.TrimSuffix(prefix, "/") + "/members/"
}

func memberKey(prefix, id string) string {
	return memberPrefix(prefix) + id
}
