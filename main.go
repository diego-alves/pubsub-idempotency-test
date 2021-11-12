package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
)

const PROJECT_ID = "chiper-poc"

func main() {
	duplication := make(map[string]bool)
	go receive_message("my-first-sub", 1*time.Second, duplication)
	receive_message("my-first-sub", 15*time.Second, duplication)
}

func receive_message(subID string, sleep time.Duration, duplication map[string]bool) {
	fmt.Printf("receiving %s %s %s\n", PROJECT_ID, subID, sleep)
	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, PROJECT_ID)
	if err != nil {
		fmt.Errorf("pubsub.NewClient: %v", err)
	}
	defer client.Close()

	var mu sync.Mutex
	sub := client.Subscription(subID)
	cctx, cancel := context.WithCancel(ctx)
	err = sub.Receive(cctx, func(ctx context.Context, msg *pubsub.Message) {
		mu.Lock()
		if duplication[string(msg.Data)] {
			fmt.Printf("This message is duplicated! %q", string(msg.Data))
		}
		duplication[string(msg.Data)] = true
		mu.Unlock()

		fmt.Printf("%s %s %q\n", subID, sleep, string(msg.Data))
		time.Sleep(sleep)
		msg.Ack()
	})
	cancel()
	if err != nil {
		fmt.Errorf("Receive: %v", err)
	}
}
