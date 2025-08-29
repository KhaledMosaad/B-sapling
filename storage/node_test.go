package storage

import (
	"reflect"
	"sync/atomic"
	"testing"
)

func TestNode_page(t *testing.T) {
	type fields struct {
		ID         uint32
		Children   []*Node
		Parent     *Node
		Typ        NodeType
		Dirty      bool
		Pairs      []Pair
		FreeLength int
	}
	type args struct {
		pageSize int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *page
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &Node{
				ID:         tt.fields.ID,
				Children:   tt.fields.Children,
				Parent:     tt.fields.Parent,
				Typ:        tt.fields.Typ,
				Dirty:      tt.fields.Dirty,
				Pairs:      tt.fields.Pairs,
				FreeLength: tt.fields.FreeLength,
			}
			got, err := n.page(tt.args.pageSize)
			if (err != nil) != tt.wantErr {
				t.Errorf("Node.page() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Node.page() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNode_Split(t *testing.T) {
	type fields struct {
		ID         uint32
		Children   []*Node
		Parent     *Node
		Typ        NodeType
		Dirty      bool
		Pairs      []Pair
		FreeLength int
	}
	type args struct {
		root      *Node
		nodeCount *atomic.Uint32
		pageSize  int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *Node
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := &Node{
				ID:         tt.fields.ID,
				Children:   tt.fields.Children,
				Parent:     tt.fields.Parent,
				Typ:        tt.fields.Typ,
				Dirty:      tt.fields.Dirty,
				Pairs:      tt.fields.Pairs,
				FreeLength: tt.fields.FreeLength,
			}
			got, err := n.Split(tt.args.root, tt.args.nodeCount, tt.args.pageSize)
			if (err != nil) != tt.wantErr {
				t.Errorf("Node.Split() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Node.Split() = %v, want %v", got, tt.want)
			}
		})
	}
}
