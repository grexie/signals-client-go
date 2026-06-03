package signalsclient

const floatTolerance = 1e-9

func positionKey(venue, instrument string) string {
	return venue + ":" + instrument
}

func sideSign(side Side) float64 {
	switch side {
	case SideBuy:
		return 1
	case SideSell:
		return -1
	default:
		return 0
	}
}

func clamp01(value float64) float64 {
	if value < 0 {
		return 0
	}
	if value > 1 {
		return 1
	}
	return value
}

func positiveOr(values ...float64) float64 {
	for _, value := range values {
		if value > 0 {
			return value
		}
	}
	return 0
}
