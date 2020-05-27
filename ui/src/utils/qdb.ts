export enum BusEvent {
  MSG_QUERY_EXPORT = "query.in.export",
  MSG_QUERY_EXEC = "query.in.exec",
  MSG_QUERY_CANCEL = "query.in.cancel",
  MSG_QUERY_RUNNING = "query.out.running",
  MSG_QUERY_OK = "query.out.ok",
  MSG_QUERY_ERROR = "query.out.error",
  MSG_QUERY_DATASET = "query.out.dataset",
  MSG_QUERY_FIND_N_EXEC = "query.build.execute",
  MSG_ACTIVE_PANEL = "active.panel",
  MSG_EDITOR_FOCUS = "editor.focus",
  MSG_EDITOR_EXECUTE = "editor.execute",
  MSG_EDITOR_EXECUTE_ALT = "editor.execute.alt",
}
