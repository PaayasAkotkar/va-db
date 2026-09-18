package vasdk1

import (
	"context"
	"fmt"
	vadb "va/app/core"

	"github.com/valkey-io/valkey-go"
)

func createKey(mode vadb.DType,
	bucket string,
	branch string,
	object string,
) string {
	return fmt.Sprintf(
		"vadb:%v:%s:%s:%s",
		mode,
		bucket,
		branch,
		object,
	)
}

func (v *IVaDB) createChannel(
	ctx context.Context,
	channel string,
	message string,
) error {
	cmd := v.core.Cli().B().
		Publish().
		Channel(channel).
		Message(message).
		Build()

	return v.core.Cli().Do(ctx, cmd).Error()
}

func (v *IVaDB) subscribeChannel(
	ctx context.Context,
	channels ...string,
) (<-chan string, <-chan error) {
	messages := make(chan string, 100)
	errors := make(chan error, 1)

	cmd := v.core.Cli().
		B().
		Subscribe().
		Channel(channels...).
		Build()

	go func() {
		defer close(messages)
		defer close(errors)

		err := v.core.Cli().Receive(
			ctx,
			cmd,
			func(message valkey.PubSubMessage) {
				select {
				case messages <- message.Message:
				case <-ctx.Done():
				}
			},
		)

		if err != nil && ctx.Err() == nil {
			errors <- err
		}
	}()

	return messages, errors
}
