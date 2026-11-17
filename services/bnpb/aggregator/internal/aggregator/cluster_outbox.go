package aggregator

import "context"

func (c *Cluster) RelayOne(ctx context.Context, p EventPublisher) (bool, error) {
	var last error

	for _, criticalOnly := range []bool{true, false} {
		for _, n := range c.Nodes {
			call, cancel := c.timeout(ctx)
			sent, err := n.Store.relayOne(call, p, criticalOnly)

			cancel()

			if sent {
				return true, nil
			}

			if err != nil {
				last = err
			}
		}

	}
	return false, last
}
