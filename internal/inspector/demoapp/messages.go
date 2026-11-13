package demoapp

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/dogmatiq/dogma"
)

func init() {
	dogma.RegisterCommand[*OpenAccount]("2c9d310b-8327-4734-989a-47be8ee95c9a")
	dogma.RegisterCommand[*Deposit]("17fc6b8c-f442-47df-a203-1b29ff1916d4")
	dogma.RegisterCommand[*Withdraw]("507af2a6-4e90-4ec5-aaaa-46124a0340e9")
	dogma.RegisterCommand[*SendStatement]("978e5cc0-2a8a-4de1-b752-c92f3255de65")

	dogma.RegisterEvent[*AccountOpened]("fcfaf1bd-9310-4135-add3-a48b9264e0a3")
	dogma.RegisterEvent[*FundsDeposited]("2c792abc-bd49-439f-9069-dd522fe592a1")
	dogma.RegisterEvent[*FundsWithdrawn]("d20c0987-c09f-4aa7-a3da-bc4796c24d81")
	dogma.RegisterEvent[*WithdrawalDeclined]("17265c8b-f00d-4887-bade-a74d0576c96b")
	dogma.RegisterEvent[*StatementSent]("19f9759c-2711-466d-82b0-9dacc58dbc52")

	dogma.RegisterDeadline[*StatementDue]("c05b65db-0084-4cba-9c96-42a0ca8efb0a")
}

// OpenAccount is a command to open a bank account.
type OpenAccount struct {
	AccountID string
	Name      string
}

// MessageDescription returns a description of the command.
func (o *OpenAccount) MessageDescription() string {
	return fmt.Sprintf("opening account %s for %s", o.AccountID, o.Name)
}

// Validate returns an error if the command is invalid.
func (o *OpenAccount) Validate(dogma.CommandValidationScope) error {
	return validateAccountID(o.AccountID)
}

// MarshalBinary returns the command's JSON representation.
func (o *OpenAccount) MarshalBinary() ([]byte, error) { return json.Marshal(o) }

// UnmarshalBinary populates the command from its JSON representation.
func (o *OpenAccount) UnmarshalBinary(data []byte) error { return json.Unmarshal(data, o) }

// Deposit is a command to add funds to an account.
type Deposit struct {
	AccountID string
	Amount    int64
}

// MessageDescription returns a description of the command.
func (d *Deposit) MessageDescription() string {
	return fmt.Sprintf("depositing %s into account %s", formatCents(d.Amount), d.AccountID)
}

// Validate returns an error if the command is invalid.
func (d *Deposit) Validate(dogma.CommandValidationScope) error {
	return validateAccountID(d.AccountID)
}

// MarshalBinary returns the command's JSON representation.
func (d *Deposit) MarshalBinary() ([]byte, error) { return json.Marshal(d) }

// UnmarshalBinary populates the command from its JSON representation.
func (d *Deposit) UnmarshalBinary(data []byte) error { return json.Unmarshal(data, d) }

// Withdraw is a command to remove funds from an account.
type Withdraw struct {
	AccountID string
	Amount    int64
}

// MessageDescription returns a description of the command.
func (w *Withdraw) MessageDescription() string {
	return fmt.Sprintf("withdrawing %s from account %s", formatCents(w.Amount), w.AccountID)
}

// Validate returns an error if the command is invalid.
func (w *Withdraw) Validate(dogma.CommandValidationScope) error {
	return validateAccountID(w.AccountID)
}

// MarshalBinary returns the command's JSON representation.
func (w *Withdraw) MarshalBinary() ([]byte, error) { return json.Marshal(w) }

// UnmarshalBinary populates the command from its JSON representation.
func (w *Withdraw) UnmarshalBinary(data []byte) error { return json.Unmarshal(data, w) }

// SendStatement is a command to send an account statement to its holder.
type SendStatement struct {
	AccountID string
	Number    int
}

// MessageDescription returns a description of the command.
func (s *SendStatement) MessageDescription() string {
	return fmt.Sprintf("sending statement #%d for account %s", s.Number, s.AccountID)
}

// Validate returns an error if the command is invalid.
func (s *SendStatement) Validate(dogma.CommandValidationScope) error {
	return validateAccountID(s.AccountID)
}

// MarshalBinary returns the command's JSON representation.
func (s *SendStatement) MarshalBinary() ([]byte, error) { return json.Marshal(s) }

// UnmarshalBinary populates the command from its JSON representation.
func (s *SendStatement) UnmarshalBinary(data []byte) error { return json.Unmarshal(data, s) }

// AccountOpened is an event that indicates that an account was opened.
type AccountOpened struct {
	AccountID string
	Name      string
}

// MessageDescription returns a description of the event.
func (a *AccountOpened) MessageDescription() string {
	return fmt.Sprintf("opened account %s for %s", a.AccountID, a.Name)
}

// Validate returns an error if the event is invalid.
func (a *AccountOpened) Validate(dogma.EventValidationScope) error {
	return validateAccountID(a.AccountID)
}

// MarshalBinary returns the event's JSON representation.
func (a *AccountOpened) MarshalBinary() ([]byte, error) { return json.Marshal(a) }

// UnmarshalBinary populates the event from its JSON representation.
func (a *AccountOpened) UnmarshalBinary(data []byte) error { return json.Unmarshal(data, a) }

// FundsDeposited is an event that indicates that funds were added to an
// account.
type FundsDeposited struct {
	AccountID string
	Amount    int64
}

// MessageDescription returns a description of the event.
func (f *FundsDeposited) MessageDescription() string {
	return fmt.Sprintf("deposited %s into account %s", formatCents(f.Amount), f.AccountID)
}

// Validate returns an error if the event is invalid.
func (f *FundsDeposited) Validate(dogma.EventValidationScope) error {
	return validateAccountID(f.AccountID)
}

// MarshalBinary returns the event's JSON representation.
func (f *FundsDeposited) MarshalBinary() ([]byte, error) { return json.Marshal(f) }

// UnmarshalBinary populates the event from its JSON representation.
func (f *FundsDeposited) UnmarshalBinary(data []byte) error { return json.Unmarshal(data, f) }

// FundsWithdrawn is an event that indicates that funds were removed from an
// account.
type FundsWithdrawn struct {
	AccountID string
	Amount    int64
}

// MessageDescription returns a description of the event.
func (f *FundsWithdrawn) MessageDescription() string {
	return fmt.Sprintf("withdrew %s from account %s", formatCents(f.Amount), f.AccountID)
}

// Validate returns an error if the event is invalid.
func (f *FundsWithdrawn) Validate(dogma.EventValidationScope) error {
	return validateAccountID(f.AccountID)
}

// MarshalBinary returns the event's JSON representation.
func (f *FundsWithdrawn) MarshalBinary() ([]byte, error) { return json.Marshal(f) }

// UnmarshalBinary populates the event from its JSON representation.
func (f *FundsWithdrawn) UnmarshalBinary(data []byte) error { return json.Unmarshal(data, f) }

// WithdrawalDeclined is an event that indicates that a withdrawal was declined
// because the account did not hold enough funds.
type WithdrawalDeclined struct {
	AccountID string
	Amount    int64
	Balance   int64
}

// MessageDescription returns a description of the event.
func (w *WithdrawalDeclined) MessageDescription() string {
	return fmt.Sprintf(
		"declined withdrawal of %s from account %s, which holds %s",
		formatCents(w.Amount),
		w.AccountID,
		formatCents(w.Balance),
	)
}

// Validate returns an error if the event is invalid.
func (w *WithdrawalDeclined) Validate(dogma.EventValidationScope) error {
	return validateAccountID(w.AccountID)
}

// MarshalBinary returns the event's JSON representation.
func (w *WithdrawalDeclined) MarshalBinary() ([]byte, error) { return json.Marshal(w) }

// UnmarshalBinary populates the event from its JSON representation.
func (w *WithdrawalDeclined) UnmarshalBinary(data []byte) error { return json.Unmarshal(data, w) }

// StatementSent is an event that indicates that an account statement was sent
// to the account's holder.
type StatementSent struct {
	AccountID string
	Number    int
}

// MessageDescription returns a description of the event.
func (s *StatementSent) MessageDescription() string {
	return fmt.Sprintf("sent statement #%d for account %s", s.Number, s.AccountID)
}

// Validate returns an error if the event is invalid.
func (s *StatementSent) Validate(dogma.EventValidationScope) error {
	return validateAccountID(s.AccountID)
}

// MarshalBinary returns the event's JSON representation.
func (s *StatementSent) MarshalBinary() ([]byte, error) { return json.Marshal(s) }

// UnmarshalBinary populates the event from its JSON representation.
func (s *StatementSent) UnmarshalBinary(data []byte) error { return json.Unmarshal(data, s) }

// StatementDue is a deadline that is reached when an account's next statement
// should be sent.
type StatementDue struct {
	AccountID string
	Number    int
}

// MessageDescription returns a description of the deadline.
func (s *StatementDue) MessageDescription() string {
	return fmt.Sprintf("statement #%d for account %s is now due", s.Number, s.AccountID)
}

// Validate returns an error if the deadline is invalid.
func (s *StatementDue) Validate(dogma.DeadlineValidationScope) error {
	return validateAccountID(s.AccountID)
}

// MarshalBinary returns the deadline's JSON representation.
func (s *StatementDue) MarshalBinary() ([]byte, error) { return json.Marshal(s) }

// UnmarshalBinary populates the deadline from its JSON representation.
func (s *StatementDue) UnmarshalBinary(data []byte) error { return json.Unmarshal(data, s) }

// validateAccountID returns an error if id is not a valid account ID.
func validateAccountID(id string) error {
	if id == "" {
		return errors.New("account ID must not be empty")
	}
	return nil
}

// formatCents returns an amount of money in cents as dollars and cents.
func formatCents(cents int64) string {
	return fmt.Sprintf("$%d.%02d", cents/100, cents%100)
}
