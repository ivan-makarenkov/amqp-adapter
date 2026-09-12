package amqpadapter

import "context"

type nilLogger struct{}

func (nilLogger) Debug(string, ...any)                                {}
func (nilLogger) Info(string, ...any)                                 {}
func (nilLogger) Warn(string, ...any)                                 {}
func (nilLogger) Error(string, ...any)                                {}
func (nilLogger) DebugContext(context.Context, string, ...any)        {}
func (nilLogger) InfoContext(context.Context, string, ...any)         {}
func (nilLogger) WarnContext(context.Context, string, ...any)         {}
func (nilLogger) ErrorContext(context.Context, string, ...any)        {}
