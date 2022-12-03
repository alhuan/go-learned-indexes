package indexes

type Model interface {
	Predict(key int64) int64
}

// Linear Regression

type LinearRegression struct {
	slope		float64
	intercept 	float64
}

func NewLinearRegression(keyValues []KeyValue, offset int64, compression_factor float64) LinearRegression {
	n := len(keyValues)
	lr := &LinearRegression{}

	if (n == 0){
		lr.slope = 0
		lr.intercept = 0
		return lr
	}
	if (n == 1){
		lr.slope = 0
		lr.intercept = offset * compression_factor
		return lr
	}

	mean_x := 0.0
	mean_y := 0.0
	c := 0.0
	m2 := 0.0

	for i, kv := range keyValues {
		y := offset + i

		dx := kv.Key - mean_x
		mean_x += dx / (i + 1)
		mean_y += (y - mean_y) / (i + 1)
		c += dx * (y - mean_y)

		dx2 := kv.Key - mean_x
		m2 += dx * dx2
	}

	covar := c / (n - 1)
	variance := m2 / (n - 1)

	if variance == 0 {
		lr.slope = 0
		lr.intercept = mean_y
	} else {
		lr.slope = covar / variance * compression_factor
		lr.intercept = mean_y * compression_factor - lr.slope * mean_x
	}
	return lr
}

func (lr *LinearRegression) Size() int64 {
	return int64(unsafe.Size(*lr))
}

func (lr *LinearRegression) Predict(key int64) int64 {
	return int64(lr.slope * x + lr.intercept);
}