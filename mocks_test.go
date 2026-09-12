package amqpadapter

import (
	"context"
	"sync"
	"time"
)

// MockLogger is a Logger implementation for tests.
type MockLogger struct {
	mu           sync.RWMutex
	infoMessages []string
	warnMessages []string
	dbgMessages  []string
	errMessages  []string
}

func NewMockLogger() *MockLogger {
	return &MockLogger{
		infoMessages: make([]string, 0),
		warnMessages: make([]string, 0),
		dbgMessages:  make([]string, 0),
		errMessages:  make([]string, 0),
	}
}

func (m *MockLogger) Info(msg string, args ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.infoMessages = append(m.infoMessages, msg)
}

func (m *MockLogger) Warn(msg string, args ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.warnMessages = append(m.warnMessages, msg)
}

func (m *MockLogger) Debug(msg string, args ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dbgMessages = append(m.dbgMessages, msg)
}

func (m *MockLogger) Error(msg string, args ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errMessages = append(m.errMessages, msg)
}

func (m *MockLogger) InfoContext(ctx context.Context, msg string, args ...any) {
	m.Info(msg, args...)
}

func (m *MockLogger) WarnContext(ctx context.Context, msg string, args ...any) {
	m.Warn(msg, args...)
}

func (m *MockLogger) DebugContext(ctx context.Context, msg string, args ...any) {
	m.Debug(msg, args...)
}

func (m *MockLogger) ErrorContext(ctx context.Context, msg string, args ...any) {
	m.Error(msg, args...)
}

func (m *MockLogger) GetInfoMessages() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]string, len(m.infoMessages))
	copy(result, m.infoMessages)
	return result
}

func (m *MockLogger) GetWarnMessages() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]string, len(m.warnMessages))
	copy(result, m.warnMessages)
	return result
}

func (m *MockLogger) GetErrorMessages() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]string, len(m.errMessages))
	copy(result, m.errMessages)
	return result
}

func (m *MockLogger) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.infoMessages = m.infoMessages[:0]
	m.warnMessages = m.warnMessages[:0]
	m.dbgMessages = m.dbgMessages[:0]
	m.errMessages = m.errMessages[:0]
}

func createTestConfig() Config {
	return Config{
		URL:            testRabbitURL,
		ReconnectDelay: 100 * time.Millisecond,
		ReInitDelay:    50 * time.Millisecond,
		ResendDelay:    10 * time.Millisecond,
		QueueParams: map[QueueName]QueueItem{
			QueueName("test_queue"): {
				Retry: &RetryConfig{
					Delay:       1 * time.Minute,
					MaxDuration: 1 * time.Hour,
				},
			},
			QueueName("test_queue_no_retry"): {},
			QueueName("test_parallel"):       {},
		},
	}
}

func createTestOptions(lgr *MockLogger) []Option {
	return []Option{
		WithLogger(lgr),
		WithFailHandler(func(job FailedJob) error { return nil }),
		WithPublishHeadersBuilder(createTestPublishHeadersBuilder()),
		WithConsumeHeadersExtractor(createTestConsumeHeadersExtractor()),
	}
}

func createTestFailJobHandler() FailJobHandler {
	return func(job FailedJob) error {
		return nil
	}
}

func createTestPublishHeadersBuilder() PublishHeadersBuilder {
	return func(ctx context.Context) map[string]any {
		return map[string]any{
			"test": "value",
		}
	}
}

func createTestConsumeHeadersExtractor() ConsumeHeadersExtractor {
	return func(parent context.Context, _ map[string]any) context.Context {
		return parent
	}
}
