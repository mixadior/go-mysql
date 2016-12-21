package canal

import (
	"time"

	"golang.org/x/net/context"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"sync"
	"github.com/siddontang/go-mysql/schema"
	"fmt"
)

func (c *Canal) startSyncBinlog() error {
	pos := *c.GetCurrentPosition()

	log.Infof("start sync binlog at %v", pos)

	s, err := c.syncer.StartSync(pos)
	if err != nil {
		return errors.Errorf("start sync replication at %v error %v", pos, err)
	}

	timeout := time.Second
	//forceSavePos := false
	xidBuf := NewXidBuffer()


		for {
			ctx, cancel := context.WithTimeout(context.Background(), 2 * time.Second)
			ev, err := s.GetEvent(ctx)
			cancel()

			if err == context.DeadlineExceeded {
				timeout = 2 * timeout
				continue
			}

			if err != nil {
				return errors.Trace(err)
			}

			timeout = time.Second

			//next binlog pos
			pos.Pos = ev.Header.LogPos

			//forceSavePos = false

			// We only save position with RotateEvent and XIDEvent.
			// For RowsEvent, we can't save the position until meeting XIDEvent
			// which tells the whole transaction is over.
			// TODO: If we meet any DDL query, we must save too.
			switch e := ev.Event.(type) {
			case *replication.RotateEvent:
				fmt.Println(e.Position, e.NextLogName)
				pos.Name = string(e.NextLogName)
				pos.Pos = uint32(e.Position)

				log.Infof("rotate binlog to %v", pos)
				c.UpdatePosition(&mysql.Position{string(e.NextLogName), uint32(e.Position)})
				evPos := &mysql.Position{string(e.NextLogName), uint32(e.Position)}
				events := newRowsEvent(&schema.Table{}, PosAction, [][]interface{}{}, evPos)
				c.travelRowsEventHandler(events)

			case *replication.RowsEvent:
				if (c.cfg.Strategy == STRATEGY_EVERY_ROW) {
					// we only focus row based event
					if err = c.handleRowsEvent(ev, &pos); err != nil {
						log.Errorf("handle rows event error %v", err)
						return errors.Trace(err)
					}
				} else if (c.cfg.Strategy == STRATEGY_XID_FLUSH) {
					xidBuf.Add(ev)
				}
				continue
			case *replication.XIDEvent:
				if (c.cfg.Strategy == STRATEGY_XID_FLUSH) {
					evs := xidBuf.Flush()
					for _, ev := range (evs) {
						if err = c.handleRowsEvent(ev, &pos); err != nil {
							log.Errorf("handle rows event error %v", err)
							return errors.Trace(err)
						}
					}
				}
			default:
				continue
			}
		}


	return nil
}

func (c *Canal) handleRowsEvent(e *replication.BinlogEvent, position *mysql.Position) error {
	ev := e.Event.(*replication.RowsEvent)

	// Caveat: table may be altered at runtime.
	schema := string(ev.Table.Schema)
	table := string(ev.Table.Table)

	t, err := c.GetTable(schema, table)
	if err != nil {
		return errors.Trace(err)
	}
	var action string
	switch e.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		action = InsertAction
	case replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		action = DeleteAction
	case replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		action = UpdateAction
	default:
		return errors.Errorf("%s not supported now", e.Header.EventType)
	}
	events := newRowsEvent(t, action, ev.Rows, position)
	return c.travelRowsEventHandler(events)
}

func (c *Canal) WaitUntilPos(pos mysql.Position, timeout int) error {
	if timeout <= 0 {
		timeout = 60
	}

	timer := time.NewTimer(time.Duration(timeout) * time.Second)
	for {
		select {
		case <-timer.C:
			return errors.Errorf("wait position %v err", pos)
		default:
			curpos := c.GetCurrentPosition()
			if curpos.Compare(pos) >= 0 {
				return nil
			} else {
				time.Sleep(100 * time.Millisecond)
			}
		}
	}

	return nil
}

func (c *Canal) CatchMasterPos() (*mysql.Position, error)  {
	var poss *mysql.Position
	rr, err := c.Execute("SHOW MASTER STATUS")
	if err != nil {
		return poss, errors.Trace(err)
	}
	name,_ := rr.GetString(0, 0)
	pos,_ := rr.GetInt(0, 1)
	return &mysql.Position{name, uint32(pos)}, err
}

func NewXidBuffer() *XidBuffer {
	x := &XidBuffer{}
	x.RWMutex = new(sync.RWMutex)
	x.BufferStart = 0
	x.BufferEnd = 0
	return x
}

type XidBuffer struct {
	*sync.RWMutex
	BufferStart uint32
	BufferEnd uint32
	Buffer []*replication.BinlogEvent
}

func (i *XidBuffer) Add(ev *replication.BinlogEvent) {
	i.Lock()
	i.Buffer = append(i.Buffer, ev)
	i.Unlock()
}

func (i *XidBuffer) Flush() []*replication.BinlogEvent {
	i.Lock()
	defer i.Unlock()
	buf := i.Buffer
	i.Buffer = []*replication.BinlogEvent{}
	return buf
}
