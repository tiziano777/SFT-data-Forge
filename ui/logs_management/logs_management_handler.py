import os
import shutil
from pathlib import Path

DELETION_LOGS_DIR = Path(__file__).parent.parent / "admin_console" / "logs"
DATATROVE_LOGS_DIR = Path(__file__).parent.parent.parent / "logs"


def show_logs_management(st):
    st.title("📜 Logs Management")

    section = st.radio(
        "Select log source",
        ["Deletion Logs", "Datatrove Pipeline Logs"],
        horizontal=True,
    )

    if section == "Deletion Logs":
        _show_deletion_logs(st)
    else:
        _show_datatrove_logs(st)


# --- Deletion Logs ---


def _show_deletion_logs(st):
    st.header("Deletion Logs")

    if not DELETION_LOGS_DIR.exists():
        st.info(f"No deletion logs directory found: `{DELETION_LOGS_DIR}`")
        return

    log_files = sorted(DELETION_LOGS_DIR.glob("*.log"), key=os.path.getmtime, reverse=True)

    if not log_files:
        st.info("No deletion log files available.")
        return

    st.write(f"**{len(log_files)}** log file(s) found.")

    for log_file in log_files:
        if st.button(f"📄 {log_file.name}", key=f"del_log_{log_file.name}"):
            st.session_state["selected_deletion_log"] = str(log_file)

    selected = st.session_state.get("selected_deletion_log")
    if selected and Path(selected).exists():
        st.subheader(f"Content: {Path(selected).name}")
        content = Path(selected).read_text(encoding="utf-8", errors="replace")
        st.code(content, language="log")


# --- Datatrove Logs ---


def _show_datatrove_logs(st):
    st.header("Datatrove Pipeline Logs")

    if not DATATROVE_LOGS_DIR.exists():
        st.info(f"No logs directory found: `{DATATROVE_LOGS_DIR}`")
        return

    # Structure: logs/<session_folder>/logs/task_*.log
    session_dirs = sorted(
        [d for d in DATATROVE_LOGS_DIR.iterdir() if d.is_dir()],
        reverse=True,
    )

    if not session_dirs:
        st.info("No session folders found in logs/.")
        return

    # Delete all sessions button
    st.write(f"**{len(session_dirs)}** session(s) in logs/")
    if st.button("🗑️ Delete ALL sessions", type="secondary"):
        st.session_state["confirm_delete_all_sessions"] = True

    if st.session_state.get("confirm_delete_all_sessions"):
        st.warning("This will permanently delete ALL session folders in logs/.")
        col_y, col_n = st.columns(2)
        with col_y:
            if st.button("✅ Confirm delete all", type="primary"):
                try:
                    for d in session_dirs:
                        shutil.rmtree(d)
                    st.success("All sessions deleted.")
                    st.session_state.pop("confirm_delete_all_sessions", None)
                    st.session_state.pop("selected_datatrove_log", None)
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to delete: {e}")
        with col_n:
            if st.button("❌ Cancel", key="cancel_delete_all"):
                st.session_state.pop("confirm_delete_all_sessions", None)
                st.rerun()

    st.divider()

    # Select session
    selected_session = st.selectbox(
        "Select session",
        session_dirs,
        format_func=lambda p: p.name,
    )

    # Find .log files inside <session>/logs/
    session_logs_dir = selected_session / "logs"
    if not session_logs_dir.exists():
        st.info(f"No logs/ subdirectory in session `{selected_session.name}`.")
        return

    all_log_files = list(session_logs_dir.glob("*.log"))

    if not all_log_files:
        st.info("No .log files found in this session.")
        return

    # Build metadata: path, line count, name
    log_entries = []
    for lf in all_log_files:
        try:
            line_count = sum(1 for _ in open(lf, "r", encoding="utf-8", errors="replace"))
        except OSError:
            line_count = 0
        log_entries.append({"path": lf, "name": lf.name, "lines": line_count})

    # --- Filters ---
    st.subheader("Filters")

    col1, col2, col3 = st.columns(3)

    with col1:
        sort_order = st.selectbox("Sort by lines", ["Descending", "Ascending"])
    with col2:
        min_lines = st.number_input("Min lines", min_value=0, value=0, step=1)
    with col3:
        max_lines = st.number_input("Max lines", min_value=0, value=0, step=1,
                                    help="0 = no upper limit")

    keyword = st.text_input("🔍 Search keyword in file content", "")

    # Apply filters
    filtered = log_entries

    if min_lines > 0:
        filtered = [e for e in filtered if e["lines"] >= min_lines]
    if max_lines > 0:
        filtered = [e for e in filtered if e["lines"] <= max_lines]

    if keyword.strip():
        keyword_lower = keyword.strip().lower()
        filtered = [
            e for e in filtered
            if keyword_lower in e["path"].read_text(encoding="utf-8", errors="replace").lower()
        ]

    # Sort
    reverse = sort_order == "Descending"
    filtered.sort(key=lambda e: e["lines"], reverse=reverse)

    st.write(f"**{len(filtered)}** file(s) matching filters (of {len(log_entries)} total)")

    # Display list
    for entry in filtered:
        label = f"📄 {entry['name']} — {entry['lines']} lines"
        if st.button(label, key=f"dt_log_{entry['name']}"):
            st.session_state["selected_datatrove_log"] = str(entry["path"])

    # Show selected file content
    selected = st.session_state.get("selected_datatrove_log")
    if selected and Path(selected).exists():
        st.subheader(f"Content: {Path(selected).name}")
        content = Path(selected).read_text(encoding="utf-8", errors="replace")
        st.code(content, language="log")

    # --- Delete session logs ---
    st.divider()
    st.subheader("⚠️ Delete Session Logs")
    st.caption("Remove the entire selected session folder to free disk space.")

    if st.button("🗑️ Delete session folder", type="secondary"):
        st.session_state["confirm_delete_datatrove_logs"] = True

    if st.session_state.get("confirm_delete_datatrove_logs"):
        st.warning(f"This will permanently delete: `{selected_session}`")
        col_yes, col_no = st.columns(2)
        with col_yes:
            if st.button("✅ Confirm delete", type="primary"):
                try:
                    shutil.rmtree(selected_session)
                    st.success("Session folder deleted.")
                    st.session_state.pop("confirm_delete_datatrove_logs", None)
                    st.session_state.pop("selected_datatrove_log", None)
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed to delete: {e}")
        with col_no:
            if st.button("❌ Cancel"):
                st.session_state.pop("confirm_delete_datatrove_logs", None)
                st.rerun()
