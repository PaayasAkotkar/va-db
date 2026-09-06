package vasdk1

import (
	"sync"
	vadb "va/app/core"
)

type IVaDB struct {
	core   *vadb.IVaDB
	happen chan bool
	mu     sync.Mutex
}

func New(c vadb.IConfig, n int) *IVaDB {
	return &IVaDB{
		core:   vadb.New(c),
		happen: make(chan bool, n),
	}
}

//
//func NewTree(c vadb.IConfig, n int) *IVaDB {
//	return &IVaDB{
//		core: vadb.New(c),
//		data: make(chan *vadb.IPush, n),
//	}
//}
//
//func Both(c vadb.IConfig, tn, kn int) *IVaDB {
//	return &IVaDB{
//		core:    vadb.New(c),
//		data:    make(chan *vadb.IPush, tn),
//		keyData: make(chan *string, kn),
//	}
//}

//type IMRole struct {
//	UserID string
//	Perms  []Perm
//}
//
//type Perm string
//
//const (
//	ADMIN Perm = "admin"
//	WO    Perm = "write_only"
//	RO    Perm = "read_only"
//	RDW   Perm = "read_write"
//)
//
//func (v *IVaDB) validRole(p Perm) bool {
//	switch p {
//	case ADMIN, WO, RO, RDW:
//		return true
//	default:
//		return false
//	}
//}
//
//type r struct {
//	roles []string
//}
//
//const (
//	hdem = ":"
//	pdem = "|"
//)
//
//func (v *IVaDB) createRolePerm(id string, op []Perm) string {
//	sh := sha256.Sum256([]byte(id))
//	hash := hex.EncodeToString(sh[:])
//	frm := hash + hdem
//	tokens := make([]string, len(op))
//	for i, r := range op {
//		if !v.validRole(r) {
//			continue
//		}
//		if r == ADMIN {
//			return frm + string(ADMIN)
//		}
//		tokens[i] = frm + string(r)
//	}
//	return strings.Join(tokens, pdem)
//}
//
//func (v *IVaDB) CreateRole(ctx context.Context, role *IMRole) error {
//	return v.core.PushKey(ctx, role.UserID, v.createRolePerm(role.UserID, role.Perms))
//}
//
//func (v *IVaDB) getPerms(ctx context.Context, id string) []Perm {
//	data, err := v.core.PullKey(ctx, id)
//	if err != nil {
//		log.Println(err)
//		return nil
//	}
//	if strings.Contains(data, string(ADMIN)) {
//		return []Perm{ADMIN}
//	}
//	sh := sha256.Sum256([]byte(id))
//	hash := hex.EncodeToString(sh[:])
//	tokens := strings.Split(data, ",")
//
//	perms := make([]Perm, 0, 1)
//
//	for _, tok := range tokens {
//		if tok == "" {
//			continue
//		}
//		p := strings.SplitN(tok, hdem, 2)
//		if len(p) != 2 {
//			continue
//		}
//		x, y := p[0], p[1]
//		if x != hash {
//			continue
//		}
//		if !v.validRole(Perm(y)) {
//			continue
//		}
//		perms = append(perms, Perm(y))
//	}
//
//	return perms
//}
//
//func (v *IVaDB) contains(src []Perm, search Perm) bool {
//	return slices.Contains(src, search)
//}
