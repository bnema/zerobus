# Dispatcher design clarifications

These review-approved corrections supersede the original Dispatcher v0 scratch
spec's sender and AddMatch cancellation rules. Other public API and FD-ownership
contracts are unchanged.

## Sender isolation (GO-001)

Bus match rules apply to a connection, not an individual subscription. Subscribe
therefore rejects a nonempty Match.Sender unless it starts with `:` (ErrInvalid).
Callers resolve well-known names using `org.freedesktop.DBus.GetNameOwner` and
pass the returned unique name. Local matching always compares a nonempty Sender
with the message's Sender, including when another subscription has a broad rule.

## Cancelled AddMatch (GO-002)

Subscribe checks ctx.Err() before sending AddMatch. If the call returns a context
cancellation/deadline error after sending, the bus may have installed the rule.
Before returning that error, Subscribe attempts RemoveMatch on the same connection
using context.WithoutCancel(ctx) with a fresh one-second timeout. Cleanup errors
are ignored: cleanup is bounded and best-effort, not a guarantee against a broken
or unresponsive bus. No cleanup goroutine is started.
