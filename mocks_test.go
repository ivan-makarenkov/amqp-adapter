package amqpadapter

import (
	"context"
	"sync"
	"time"
)

// MockLogger реализация интерфейса Logger для тестов.
type MockLogger struct {
	mu          sync.RWMutex
	infMessages []string
	wrnMessages []string
	dbgMessages []string
	errMessages []string
	ftlMessages []string
}

func NewMockLogger() *MockLogger {
	return &MockLogger{
		infMessages: make([]string, 0),
		wrnMessages: make([]string, 0),
		dbgMessages: make([]string, 0),
		errMessages: make([]string, 0),
		ftlMessages: make([]string, 0),
	}
}

func (m *MockLogger) Inf(msg string, args ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.infMessages = append(m.infMessages, msg)
}

func (m *MockLogger) Wrn(msg string, args ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.wrnMessages = append(m.wrnMessages, msg)
}

func (m *MockLogger) Dbg(msg string, args ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dbgMessages = append(m.dbgMessages, msg)
}

func (m *MockLogger) Err(msg string, args ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errMessages = append(m.errMessages, msg)
}

func (m *MockLogger) Ftl(msg string, args ...any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ftlMessages = append(m.ftlMessages, msg)
}

func (m *MockLogger) InfCtx(ctx context.Context, msg string, args ...any) {
	m.Inf(msg, args...)
}

func (m *MockLogger) WrnCtx(ctx context.Context, msg string, args ...any) {
	m.Wrn(msg, args...)
}

func (m *MockLogger) DbgCtx(ctx context.Context, msg string, args ...any) {
	m.Dbg(msg, args...)
}

func (m *MockLogger) ErrCtx(ctx context.Context, msg string, args ...any) {
	m.Err(msg, args...)
}

func (m *MockLogger) FtlCtx(ctx context.Context, msg string, args ...any) {
	m.Ftl(msg, args...)
}

func (m *MockLogger) GetInfMessages() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]string, len(m.infMessages))
	copy(result, m.infMessages)
	return result
}

func (m *MockLogger) GetWrnMessages() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]string, len(m.wrnMessages))
	copy(result, m.wrnMessages)
	return result
}

func (m *MockLogger) GetErrMessages() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]string, len(m.errMessages))
	copy(result, m.errMessages)
	return result
}

func (m *MockLogger) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.infMessages = m.infMessages[:0]
	m.wrnMessages = m.wrnMessages[:0]
	m.dbgMessages = m.dbgMessages[:0]
	m.errMessages = m.errMessages[:0]
	m.ftlMessages = m.ftlMessages[:0]
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
