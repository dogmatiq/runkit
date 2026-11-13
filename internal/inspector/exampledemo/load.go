package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/example/messages/commands"
	"github.com/dogmatiq/protobuf/uuidpb"
)

// commandInterval is the average time between the commands that the load
// executes.
const commandInterval = 250 * time.Millisecond

// customer is a customer that the load opened, and the accounts it opened for
// them.
type customer struct {
	ID       string
	Accounts []string
}

// executeCommands executes commands that a bank's customers would cause, until
// ctx is canceled. It opens customers and accounts, deposits and withdraws
// money, and transfers money between accounts and to the third-party bank,
// sometimes scheduled for later.
func executeCommands(ctx context.Context, executor dogma.CommandExecutor) {
	var customers []*customer

	for {
		delay := time.Duration(rand.Int64N(int64(2 * commandInterval)))

		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}

		var command dogma.Command

		switch {
		case len(customers) == 0 || rand.IntN(12) == 0:
			c := &customer{
				ID:       uuidpb.Generate().AsString(),
				Accounts: []string{newAccountID()},
			}
			customers = append(customers, c)

			command = &commands.OpenAccountForNewCustomer{
				CustomerID:   c.ID,
				CustomerName: holders[rand.IntN(len(holders))],
				AccountID:    c.Accounts[0],
				AccountName:  "Everyday",
			}

		case rand.IntN(20) == 0:
			c := customers[rand.IntN(len(customers))]
			id := newAccountID()
			c.Accounts = append(c.Accounts, id)

			command = &commands.OpenAccount{
				CustomerID:  c.ID,
				AccountID:   id,
				AccountName: "Savings",
			}

		default:
			command = moneyCommand(customers)
		}

		// A node that is stopping can not execute commands, so failures are
		// expected and ignored.
		executor.ExecuteCommand(ctx, command)
	}
}

// moneyCommand returns a deposit, a withdrawal or a transfer involving the
// accounts of the given customers.
func moneyCommand(customers []*customer) dogma.Command {
	from := randomAccount(customers)
	amount := 100 * (1 + rand.Int64N(500))

	switch rand.IntN(6) {
	case 0, 1:
		return &commands.Deposit{
			TransactionID: uuidpb.Generate().AsString(),
			AccountID:     from,
			Amount:        amount * 3,
		}

	case 2:
		return &commands.Withdraw{
			TransactionID: uuidpb.Generate().AsString(),
			AccountID:     from,
			Amount:        amount,
			ScheduledTime: time.Now(),
		}

	case 3:
		return &commands.Transfer{
			TransactionID:    uuidpb.Generate().AsString(),
			FromAccountID:    from,
			ToAccountID:      newAccountID(),
			ToThirdPartyBank: true,
			Amount:           amount,
			ScheduledTime:    time.Now(),
		}

	default:
		scheduled := time.Now()
		if rand.IntN(4) == 0 {
			scheduled = scheduled.Add(10 * time.Second)
		}

		return &commands.Transfer{
			TransactionID: uuidpb.Generate().AsString(),
			FromAccountID: from,
			ToAccountID:   randomAccount(customers),
			Amount:        amount,
			ScheduledTime: scheduled,
		}
	}
}

// randomAccount returns the ID of one of the given customers' accounts.
func randomAccount(customers []*customer) string {
	c := customers[rand.IntN(len(customers))]
	return c.Accounts[rand.IntN(len(c.Accounts))]
}

// newAccountID returns a new account ID, a nine-digit number as the bank's
// user interface makes them.
func newAccountID() string {
	return fmt.Sprintf("%09d", rand.IntN(1_000_000_000))
}

// holders is the names of the load's customers.
var holders = []string{
	"Alex", "Bea", "Chidi", "Dana", "Eli", "Fatima", "Goran", "Hana",
	"Ines", "Jun", "Kofi", "Lena", "Mateo", "Noor", "Oskar", "Priya",
}
