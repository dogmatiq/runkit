// Package demoapp is a small banking application that exercises every type of
// Dogma handler. The inspector's tests and its demo cluster run it.
package demoapp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/dogmatiq/dogma"
)

// App is a Dogma application that manages bank accounts. Opening an account
// starts a process instance that sends three statements, one at a time, each
// [App.StatementInterval] after the previous one.
type App struct {
	// StatementInterval is the time between the statements of an account.
	StatementInterval time.Duration

	// MailerFailureRate is the probability, between 0 and 1, that an attempt
	// to send a statement fails.
	MailerFailureRate float64

	// Balances is the projection of the balance of each account.
	Balances BalanceProjection
}

// Configure declares the application's configuration.
func (a *App) Configure(c dogma.ApplicationConfigurer) {
	c.Identity("statements", "d0137119-2660-4bc2-ab4c-bc156e08521c")
	c.Routes(
		dogma.ViaAggregate[*account](&accountHandler{}),
		dogma.ViaProcess[*statementSchedule](
			&statementHandler{
				Interval: a.StatementInterval,
			},
		),
		dogma.ViaIntegration(
			&mailerHandler{
				FailureRate: a.MailerFailureRate,
			},
		),
		dogma.ViaProjection(&a.Balances),
	)
}

// accountHandler is the aggregate message handler for bank accounts.
type accountHandler struct{}

func (a *accountHandler) Configure(c dogma.AggregateConfigurer) {
	c.Identity("account", "23b6d1c5-a4d0-4b15-8424-3557e59f962e")
	c.Routes(
		dogma.HandlesCommand[*OpenAccount](),
		dogma.HandlesCommand[*Deposit](),
		dogma.HandlesCommand[*Withdraw](),
		dogma.RecordsEvent[*AccountOpened](),
		dogma.RecordsEvent[*FundsDeposited](),
		dogma.RecordsEvent[*FundsWithdrawn](),
		dogma.RecordsEvent[*WithdrawalDeclined](),
	)
}

func (a *accountHandler) New() *account {
	return &account{}
}

func (a *accountHandler) RouteCommandToInstance(c dogma.Command) string {
	switch c := c.(type) {
	case *OpenAccount:
		return c.AccountID
	case *Deposit:
		return c.AccountID
	case *Withdraw:
		return c.AccountID
	default:
		panic(fmt.Sprintf("unexpected command: %T", c))
	}
}

func (a *accountHandler) HandleCommand(
	r *account,
	s dogma.AggregateCommandScope[*account],
	c dogma.Command,
) {
	switch c := c.(type) {
	case *OpenAccount:
		if !r.Opened {
			s.RecordEvent(
				&AccountOpened{
					AccountID: c.AccountID,
					Name:      c.Name,
				},
			)
		}

	case *Deposit:
		s.RecordEvent(
			&FundsDeposited{
				AccountID: c.AccountID,
				Amount:    c.Amount,
			},
		)

	case *Withdraw:
		if r.Balance < c.Amount {
			s.RecordEvent(
				&WithdrawalDeclined{
					AccountID: c.AccountID,
					Amount:    c.Amount,
					Balance:   r.Balance,
				},
			)
			return
		}

		s.RecordEvent(
			&FundsWithdrawn{
				AccountID: c.AccountID,
				Amount:    c.Amount,
			},
		)
	}
}

// account is the aggregate root of a bank account.
type account struct {
	Name    string
	Opened  bool
	Balance int64
}

func (a *account) AggregateInstanceDescription() string {
	if !a.Opened {
		return ""
	}
	return fmt.Sprintf("held by %s, balance %s", a.Name, formatCents(a.Balance))
}

func (a *account) ApplyEvent(e dogma.Event) {
	switch e := e.(type) {
	case *AccountOpened:
		a.Name = e.Name
		a.Opened = true
	case *FundsDeposited:
		a.Balance += e.Amount
	case *FundsWithdrawn:
		a.Balance -= e.Amount
	}
}

func (a *account) MarshalBinary() ([]byte, error) { return json.Marshal(a) }

func (a *account) UnmarshalBinary(data []byte) error { return json.Unmarshal(data, a) }

// statementHandler is the process message handler that schedules the
// statements of each account.
type statementHandler struct {
	// Interval is the time between the statements of an account.
	Interval time.Duration
}

func (h *statementHandler) Configure(c dogma.ProcessConfigurer) {
	c.Identity("statement-schedule", "9135fb61-68a0-4b23-9bea-6357ae813804")
	c.Routes(
		dogma.HandlesEvent[*AccountOpened](),
		dogma.HandlesEvent[*StatementSent](),
		dogma.ExecutesCommand[*SendStatement](),
		dogma.SchedulesDeadline[*StatementDue](),
	)
}

func (h *statementHandler) New() *statementSchedule {
	return &statementSchedule{}
}

func (h *statementHandler) RouteEventToInstance(
	_ context.Context,
	e dogma.Event,
) (string, bool, error) {
	switch e := e.(type) {
	case *AccountOpened:
		return e.AccountID, true, nil
	case *StatementSent:
		return e.AccountID, true, nil
	default:
		return "", false, nil
	}
}

func (h *statementHandler) HandleEvent(
	_ context.Context,
	r *statementSchedule,
	s dogma.ProcessEventScope[*statementSchedule],
	e dogma.Event,
) error {
	switch e := e.(type) {
	case *AccountOpened:
		s.Mutate(
			func(r *statementSchedule) {
				r.AccountID = e.AccountID
			},
		)
		s.ScheduleDeadline(
			&StatementDue{
				AccountID: e.AccountID,
				Number:    1,
			},
			s.Now().Add(h.Interval),
		)

	case *StatementSent:
		s.Mutate(
			func(r *statementSchedule) {
				r.Sent = e.Number
			},
		)

		if e.Number == statementsPerAccount {
			s.End()
			return nil
		}

		s.ScheduleDeadline(
			&StatementDue{
				AccountID: e.AccountID,
				Number:    e.Number + 1,
			},
			s.Now().Add(h.Interval),
		)
	}

	return nil
}

func (h *statementHandler) HandleDeadline(
	_ context.Context,
	r *statementSchedule,
	s dogma.ProcessDeadlineScope[*statementSchedule],
	d dogma.Deadline,
) error {
	due := d.(*StatementDue)

	s.ExecuteCommand(
		&SendStatement{
			AccountID: due.AccountID,
			Number:    due.Number,
		},
	)

	return nil
}

// statementsPerAccount is the number of statements sent for each account.
const statementsPerAccount = 3

// statementSchedule is the process root that schedules an account's
// statements.
type statementSchedule struct {
	AccountID string
	Sent      int
}

func (s *statementSchedule) ProcessInstanceDescription(ended bool) string {
	if ended {
		return fmt.Sprintf("sent all %d statements", s.Sent)
	}
	return fmt.Sprintf("sent %d of %d statements", s.Sent, statementsPerAccount)
}

func (s *statementSchedule) MarshalBinary() ([]byte, error) { return json.Marshal(s) }

func (s *statementSchedule) UnmarshalBinary(data []byte) error { return json.Unmarshal(data, s) }

// mailerHandler is the integration message handler that sends statements.
type mailerHandler struct {
	// FailureRate is the probability, between 0 and 1, that an attempt to send
	// a statement fails.
	FailureRate float64
}

func (m *mailerHandler) Configure(c dogma.IntegrationConfigurer) {
	c.Identity("mailer", "7e958523-aef2-4ef4-bbff-a7c000ed4df5")
	c.ConcurrencyPreference(dogma.MinimizeConcurrency)
	c.Routes(
		dogma.HandlesCommand[*SendStatement](),
		dogma.RecordsEvent[*StatementSent](),
	)
}

func (m *mailerHandler) HandleCommand(
	_ context.Context,
	s dogma.IntegrationCommandScope,
	c dogma.Command,
) error {
	send := c.(*SendStatement)

	if rand.Float64() < m.FailureRate {
		return errors.New("mail server refused the connection")
	}

	s.RecordEvent(
		&StatementSent{
			AccountID: send.AccountID,
			Number:    send.Number,
		},
	)

	return nil
}

// BalanceProjection is a projection of the balance of each account, held in
// memory.
type BalanceProjection struct {
	dogma.NoCompactBehavior
	dogma.NoResetBehavior

	m        sync.Mutex
	balances map[string]int64
	offsets  map[string]uint64
}

// Configure declares the handler's configuration.
func (b *BalanceProjection) Configure(c dogma.ProjectionConfigurer) {
	c.Identity("balances", "92be8333-ca3b-4b3f-8ba9-d0d1efae25cc")
	c.Routes(
		dogma.HandlesEvent[*FundsDeposited](),
		dogma.HandlesEvent[*FundsWithdrawn](),
	)
}

// HandleEvent updates the balance of the event's account.
func (b *BalanceProjection) HandleEvent(
	_ context.Context,
	s dogma.ProjectionEventScope,
	e dogma.Event,
) (uint64, error) {
	b.m.Lock()
	defer b.m.Unlock()

	if b.offsets == nil {
		b.offsets = map[string]uint64{}
		b.balances = map[string]int64{}
	}

	cp := b.offsets[s.StreamID()]
	if cp != s.CheckpointOffset() {
		return cp, nil
	}

	switch e := e.(type) {
	case *FundsDeposited:
		b.balances[e.AccountID] += e.Amount
	case *FundsWithdrawn:
		b.balances[e.AccountID] -= e.Amount
	}

	cp = s.Offset() + 1
	b.offsets[s.StreamID()] = cp

	return cp, nil
}

// CheckpointOffset returns the offset at which the projection resumes handling
// events from the stream with the given ID.
func (b *BalanceProjection) CheckpointOffset(_ context.Context, id string) (uint64, error) {
	b.m.Lock()
	defer b.m.Unlock()

	return b.offsets[id], nil
}
