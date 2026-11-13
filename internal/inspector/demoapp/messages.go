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
func (c *OpenAccount) MessageDescription() string {
	return fmt.Sprintf("opening account %s for %s", c.AccountID, c.Name)
}

// Validate returns an error if the command is invalid.
func (c *OpenAccount) Validate(dogma.CommandValidationScope) error {
	return validateAccountID(c.AccountID)
}

// MarshalBinary returns the command's JSON representation.
func (c *OpenAccount) MarshalBinary() ([]byte, error) { return json.Marshal(c) }

// UnmarshalBinary populates the command from its JSON representation.
func (c *OpenAccount) UnmarshalBinary(data []byte) error { return json.Unmarshal(data, c) }

// Deposit is a command to add funds to an account.
type Deposit struct {
	AccountID string
	Amount    int64
}

// MessageDescription returns a description of the command.
func (c *Deposit) MessageDescription() string {
	return fmt.Sprintf("depositing %s into account %s", formatCents(c.Amount), c.AccountID)
}

// Validate returns an error if the command is invalid.
func (c *Deposit) Validate(dogma.CommandValidationScope) error {
	return validateAccountID(c.AccountID)
}

// MarshalBinary returns the command's JSON representation.
func (c *Deposit) MarshalBinary() ([]byte, error) { return json.Marshal(c) }

// UnmarshalBinary populates the command from its JSON representation.
func (c *Deposit) UnmarshalBinary(data []byte) error { return json.Unmarshal(data, c) }

// Withdraw is a command to remove funds from an account.
type Withdraw struct {
	AccountID string
	Amount    int64
}

// MessageDescription returns a description of the command.
func (c *Withdraw) MessageDescription() string {
	return fmt.Sprintf("withdrawing %s from account %s", formatCents(c.Amount), c.AccountID)
}

// Validate returns an error if the command is invalid.
func (c *Withdraw) Validate(dogma.CommandValidationScope) error {
	return validateAccountID(c.AccountID)
}

// MarshalBinary returns the command's JSON representation.
func (c *Withdraw) MarshalBinary() ([]byte, error) { return json.Marshal(c) }

// UnmarshalBinary populates the command from its JSON representation.
func (c *Withdraw) UnmarshalBinary(data []byte) error { return json.Unmarshal(data, c) }

// SendStatement is a command to send an account statement to its holder.
type SendStatement struct {
	AccountID string
	Number    int
}

// MessageDescription returns a description of the command.
func (c *SendStatement) MessageDescription() string {
	return fmt.Sprintf("sending statement #%d for account %s", c.Number, c.AccountID)
}

// Validate returns an error if the command is invalid.
func (c *SendStatement) Validate(dogma.CommandValidationScope) error {
	return validateAccountID(c.AccountID)
}

// MarshalBinary returns the command's JSON representation.
func (c *SendStatement) MarshalBinary() ([]byte, error) { return json.Marshal(c) }

// UnmarshalBinary populates the command from its JSON representation.
func (c *SendStatement) UnmarshalBinary(data []byte) error { return json.Unmarshal(data, c) }

// AccountOpened is an event that indicates that an account was opened.
type AccountOpened struct {
	AccountID string
	Name      string
}

// MessageDescription returns a description of the event.
func (e *AccountOpened) MessageDescription() string {
	return fmt.Sprintf("opened account %s for %s", e.AccountID, e.Name)
}

// Validate returns an error if the event is invalid.
func (e *AccountOpened) Validate(dogma.EventValidationScope) error {
	return validateAccountID(e.AccountID)
}

// MarshalBinary returns the event's JSON representation.
func (e *AccountOpened) MarshalBinary() ([]byte, error) { return json.Marshal(e) }

// UnmarshalBinary populates the event from its JSON representation.
func (e *AccountOpened) UnmarshalBinary(data []byte) error { return json.Unmarshal(data, e) }

// FundsDeposited is an event that indicates that funds were added to an
// account.
type FundsDeposited struct {
	AccountID string
	Amount    int64
}

// MessageDescription returns a description of the event.
func (e *FundsDeposited) MessageDescription() string {
	return fmt.Sprintf("deposited %s into account %s", formatCents(e.Amount), e.AccountID)
}

// Validate returns an error if the event is invalid.
func (e *FundsDeposited) Validate(dogma.EventValidationScope) error {
	return validateAccountID(e.AccountID)
}

// MarshalBinary returns the event's JSON representation.
func (e *FundsDeposited) MarshalBinary() ([]byte, error) { return json.Marshal(e) }

// UnmarshalBinary populates the event from its JSON representation.
func (e *FundsDeposited) UnmarshalBinary(data []byte) error { return json.Unmarshal(data, e) }

// FundsWithdrawn is an event that indicates that funds were removed from an
// account.
type FundsWithdrawn struct {
	AccountID string
	Amount    int64
}

// MessageDescription returns a description of the event.
func (e *FundsWithdrawn) MessageDescription() string {
	return fmt.Sprintf("withdrew %s from account %s", formatCents(e.Amount), e.AccountID)
}

// Validate returns an error if the event is invalid.
func (e *FundsWithdrawn) Validate(dogma.EventValidationScope) error {
	return validateAccountID(e.AccountID)
}

// MarshalBinary returns the event's JSON representation.
func (e *FundsWithdrawn) MarshalBinary() ([]byte, error) { return json.Marshal(e) }

// UnmarshalBinary populates the event from its JSON representation.
func (e *FundsWithdrawn) UnmarshalBinary(data []byte) error { return json.Unmarshal(data, e) }

// WithdrawalDeclined is an event that indicates that a withdrawal was declined
// because the account did not hold enough funds.
type WithdrawalDeclined struct {
	AccountID string
	Amount    int64
	Balance   int64
}

// MessageDescription returns a description of the event.
func (e *WithdrawalDeclined) MessageDescription() string {
	return fmt.Sprintf(
		"declined withdrawal of %s from account %s, which holds %s",
		formatCents(e.Amount),
		e.AccountID,
		formatCents(e.Balance),
	)
}

// Validate returns an error if the event is invalid.
func (e *WithdrawalDeclined) Validate(dogma.EventValidationScope) error {
	return validateAccountID(e.AccountID)
}

// MarshalBinary returns the event's JSON representation.
func (e *WithdrawalDeclined) MarshalBinary() ([]byte, error) { return json.Marshal(e) }

// UnmarshalBinary populates the event from its JSON representation.
func (e *WithdrawalDeclined) UnmarshalBinary(data []byte) error { return json.Unmarshal(data, e) }

// StatementSent is an event that indicates that an account statement was sent
// to the account's holder.
type StatementSent struct {
	AccountID string
	Number    int
}

// MessageDescription returns a description of the event.
func (e *StatementSent) MessageDescription() string {
	return fmt.Sprintf("sent statement #%d for account %s", e.Number, e.AccountID)
}

// Validate returns an error if the event is invalid.
func (e *StatementSent) Validate(dogma.EventValidationScope) error {
	return validateAccountID(e.AccountID)
}

// MarshalBinary returns the event's JSON representation.
func (e *StatementSent) MarshalBinary() ([]byte, error) { return json.Marshal(e) }

// UnmarshalBinary populates the event from its JSON representation.
func (e *StatementSent) UnmarshalBinary(data []byte) error { return json.Unmarshal(data, e) }

// StatementDue is a deadline that is reached when an account's next statement
// should be sent.
type StatementDue struct {
	AccountID string
	Number    int
}

// MessageDescription returns a description of the deadline.
func (d *StatementDue) MessageDescription() string {
	return fmt.Sprintf("statement #%d for account %s is now due", d.Number, d.AccountID)
}

// Validate returns an error if the deadline is invalid.
func (d *StatementDue) Validate(dogma.DeadlineValidationScope) error {
	return validateAccountID(d.AccountID)
}

// MarshalBinary returns the deadline's JSON representation.
func (d *StatementDue) MarshalBinary() ([]byte, error) { return json.Marshal(d) }

// UnmarshalBinary populates the deadline from its JSON representation.
func (d *StatementDue) UnmarshalBinary(data []byte) error { return json.Unmarshal(data, d) }

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
