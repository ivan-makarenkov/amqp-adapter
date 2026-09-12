package amqpadapter

import "context"

type nilLogger struct{}

func (nilLogger) Inf(string, ...any)                     {}
func (nilLogger) Wrn(string, ...any)                     {}
func (nilLogger) Dbg(string, ...any)                     {}
func (nilLogger) Err(string, ...any)                     {}
func (nilLogger) Ftl(string, ...any)                     {}
func (nilLogger) InfCtx(context.Context, string, ...any) {}
func (nilLogger) WrnCtx(context.Context, string, ...any) {}
func (nilLogger) DbgCtx(context.Context, string, ...any) {}
func (nilLogger) ErrCtx(context.Context, string, ...any) {}
func (nilLogger) FtlCtx(context.Context, string, ...any) {}
