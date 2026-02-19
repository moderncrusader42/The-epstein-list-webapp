from __future__ import annotations

import logging
from pathlib import Path

import gradio as gr

from src.page_timing import timed_page_load
from src.pages.header import render_header, with_light_mode_head
from src.pages.unsorted_files.core_unsorted_files import (
    _cancel_unsorted_tags_modal,
    _cancel_unsorted_push_modal,
    _close_unsorted_upload_panel,
    _load_unsorted_files_page,
    _mark_unsorted_too_redacted,
    _mark_unsorted_useless,
    _next_unsorted_file,
    _open_unsorted_tags_modal,
    _open_unsorted_push_modal,
    _open_unsorted_upload_panel,
    _previous_unsorted_file,
    _submit_unsorted_tags_proposal,
    _submit_unsorted_push_to_source,
    _upload_unsorted_files,
)

logger = logging.getLogger(__name__)

ASSETS_DIR = Path(__file__).resolve().parent
CSS_PATH = ASSETS_DIR / "css" / "unsorted_files_page.css"
JS_PATH = ASSETS_DIR / "js" / "unsorted_files_page.js"
TAGS_JS_PATH = ASSETS_DIR / "js" / "unsorted_tags_editor.js"


def _read_asset(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        logger.warning("Missing unsorted files asset at %s", path)
        return ""


def _load_css() -> str:
    return _read_asset(CSS_PATH)


def _load_script(path: Path) -> str:
    script = _read_asset(path)
    if not script:
        return ""
    return f"<script>\n{script}\n</script>"


def _header_unsorted_files(request: gr.Request):
    return render_header(path="/unsorted-files", request=request)


def _has_unsorted_upload_value(upload_value: object) -> bool:
    if upload_value is None:
        return False
    if isinstance(upload_value, (list, tuple)):
        return len(upload_value) > 0
    return bool(upload_value)


def _toggle_unsorted_upload_mode(mode: str, upload_files: object, upload_folder: object):
    normalized = str(mode or "files").strip().lower()
    has_files = _has_unsorted_upload_value(upload_files)
    has_folder = _has_unsorted_upload_value(upload_folder)
    if normalized == "folder":
        return (
            gr.update(visible=has_files),
            gr.update(visible=True),
            gr.update(
                value=(
                    "Folder mode: select one folder and every nested file will be flattened into this batch. "
                    "If Files already has selections, it stays visible and uploads together."
                )
            ),
        )
    return (
        gr.update(visible=True),
        gr.update(visible=has_folder),
        gr.update(
            value=(
                "Files mode: add one or many standalone files in a single batch. "
                "If Folder already has selections, it stays visible and uploads together."
            )
        ),
    )


def _reset_unsorted_upload_mode():
    return (
        gr.update(value="files"),
        gr.update(visible=True, value=None),
        gr.update(visible=False, value=None),
        gr.update(value="Files mode: add one or many standalone files in a single batch."),
    )


def _start_unsorted_upload():
    return (
        gr.update(value="Uploading unsorted files... please wait.", visible=True),
        gr.update(value="Uploading...", interactive=False),
        gr.update(interactive=False),
    )


def make_unsorted_files_app() -> gr.Blocks:
    stylesheet = _load_css()
    scripts = "\n".join(part for part in (_load_script(JS_PATH), _load_script(TAGS_JS_PATH)) if part)

    with gr.Blocks(
        title="Unsorted files",
        css=stylesheet or None,
        head=with_light_mode_head(scripts or None),
    ) as app:
        hdr = gr.HTML()

        can_submit_state = gr.State(False)
        is_admin_state = gr.State(False)
        files_state = gr.State([])
        current_index_state = gr.State(0)
        current_file_id_state = gr.State(0)

        with gr.Column(elem_id="unsorted-shell"):
            with gr.Row(elem_id="unsorted-title-row"):
                gr.HTML("<h2>Unsorted files</h2>", elem_id="unsorted-title")
                upload_open_btn = gr.Button(
                    "Upload",
                    variant="secondary",
                    elem_id="unsorted-upload-trigger",
                    scale=0,
                    min_width=88,
                    visible=False,
                )

            with gr.Column(elem_id="unsorted-upload-panel", visible=False) as upload_panel:
                with gr.Row(elem_id="unsorted-upload-top-row"):
                    gr.Markdown("### Upload unsorted files")
                    upload_mode = gr.Radio(
                        choices=[("Files", "files"), ("Folder", "folder")],
                        value="files",
                        show_label=False,
                        container=False,
                        interactive=True,
                        elem_id="unsorted-upload-mode",
                        scale=0,
                        min_width=160,
                    )

                upload_status = gr.Markdown(value="", visible=False, elem_id="unsorted-upload-status")
                upload_mode_hint = gr.Markdown(
                    value="Files mode: add one or many standalone files in a single batch.",
                    elem_id="unsorted-upload-mode-hint",
                )
                upload_files = gr.File(
                    label="Upload files",
                    file_count="multiple",
                    file_types=["image", "video", ".pdf", ".txt", ".md", ".csv", ".json"],
                    elem_id="unsorted-upload-files",
                    visible=True,
                )
                upload_folder = gr.File(
                    label="Upload folder",
                    file_count="directory",
                    elem_id="unsorted-upload-folder",
                    visible=False,
                )
                upload_origin = gr.Textbox(
                    label="Description/Origin",
                    placeholder="Shared origin/description for all uploaded files in this batch.",
                    lines=2,
                    elem_id="unsorted-upload-origin",
                )
                with gr.Row(elem_id="unsorted-upload-actions"):
                    upload_submit_btn = gr.Button("Upload", variant="primary")
                    upload_cancel_btn = gr.Button("Cancel", variant="secondary")

            explorer_view_html = gr.HTML(elem_id="unsorted-explorer-view")

            with gr.Row(elem_id="unsorted-review-shell", visible=False) as review_shell:
                with gr.Column(elem_id="unsorted-preview-col"):
                    file_preview_html = gr.HTML(elem_id="unsorted-file-preview")

                with gr.Column(elem_id="unsorted-side-col"):
                    file_meta_html = gr.HTML(elem_id="unsorted-file-meta")

                    with gr.Row(elem_id="unsorted-nav-row"):
                        prev_btn = gr.Button(
                            "←",
                            variant="secondary",
                            elem_id="unsorted-prev-btn",
                            scale=0,
                            min_width=58,
                        )
                        file_counter = gr.Markdown("0 / 0", elem_id="unsorted-file-counter")
                        next_btn = gr.Button(
                            "→",
                            variant="secondary",
                            elem_id="unsorted-next-btn",
                            scale=0,
                            min_width=58,
                        )

                    current_action_md = gr.Markdown(value="", visible=False, elem_id="unsorted-current-action")

                    with gr.Row(elem_id="unsorted-actions-row"):
                        too_redacted_btn = gr.Button(
                            "Too redacted (0)",
                            variant="secondary",
                            elem_id="unsorted-too-redacted-btn",
                        )
                        push_to_source_btn = gr.Button(
                            "Push to source",
                            variant="secondary",
                            elem_id="unsorted-push-btn",
                        )
                        tag_file_btn = gr.Button(
                            "Add tags",
                            variant="secondary",
                            elem_id="unsorted-tags-btn",
                            visible=False,
                        )
                        create_source_link = gr.HTML("", elem_id="unsorted-create-source-link")
                        useless_btn = gr.Button("Useless (0)", variant="secondary", elem_id="unsorted-useless-btn")

                    action_status = gr.Markdown(value="", visible=False, elem_id="unsorted-action-status")

        with gr.Column(elem_id="unsorted-push-modal-overlay", visible=False) as push_modal:
            with gr.Column(elem_id="unsorted-push-modal"):
                gr.Markdown("### Push to source")
                push_status = gr.Markdown(value="", visible=False, elem_id="unsorted-push-status")
                push_source_dropdown = gr.Dropdown(
                    label="Select source",
                    choices=[],
                    value=None,
                    interactive=False,
                    elem_id="unsorted-push-source-selector",
                )
                push_note = gr.Textbox(
                    label="Optional note",
                    placeholder="Reason or context for the push proposal...",
                    lines=2,
                    elem_id="unsorted-push-note",
                )
                with gr.Row(elem_id="unsorted-push-actions"):
                    push_confirm_btn = gr.Button("Submit proposal", variant="primary")
                    push_cancel_btn = gr.Button("Cancel", variant="secondary")

        with gr.Column(elem_id="unsorted-tags-modal", visible=False) as tags_modal:
            gr.Markdown("### Add tags")
            tags_status = gr.Markdown(value="", visible=False, elem_id="unsorted-tags-status")
            tags_input = gr.Textbox(
                value="",
                visible=False,
                interactive=True,
                elem_id="unsorted-tags-input",
            )
            tags_editor = gr.HTML(value="", elem_id="unsorted-tags-editor-shell")
            tags_note = gr.Textbox(
                label="Optional note",
                placeholder="Reason or context for this tag proposal...",
                lines=2,
                elem_id="unsorted-tags-note",
            )
            with gr.Row(elem_id="unsorted-tags-actions"):
                tags_confirm_btn = gr.Button(
                    "Submit proposal",
                    variant="primary",
                    elem_id="unsorted-tags-submit-btn",
                )
                tags_cancel_btn = gr.Button("Cancel", variant="secondary")

        app.load(timed_page_load("/unsorted-files", _header_unsorted_files), outputs=[hdr])

        app.load(
            timed_page_load("/unsorted-files", _load_unsorted_files_page),
            outputs=[
                can_submit_state,
                is_admin_state,
                upload_open_btn,
                upload_panel,
                upload_status,
                files_state,
                current_index_state,
                current_file_id_state,
                explorer_view_html,
                review_shell,
                file_preview_html,
                file_meta_html,
                file_counter,
                current_action_md,
                prev_btn,
                next_btn,
                too_redacted_btn,
                push_to_source_btn,
                tag_file_btn,
                useless_btn,
                create_source_link,
                action_status,
                push_modal,
                push_status,
                push_source_dropdown,
                push_note,
                tags_modal,
                tags_status,
                tags_input,
                tags_editor,
                tags_note,
            ],
        )

        upload_mode.change(
            _toggle_unsorted_upload_mode,
            inputs=[upload_mode, upload_files, upload_folder],
            outputs=[upload_files, upload_folder, upload_mode_hint],
            show_progress=False,
        )

        prev_btn.click(
            timed_page_load("/unsorted-files", _previous_unsorted_file, label="previous_unsorted_file"),
            inputs=[files_state, current_index_state, can_submit_state],
            outputs=[
                current_index_state,
                current_file_id_state,
                explorer_view_html,
                review_shell,
                file_preview_html,
                file_meta_html,
                file_counter,
                current_action_md,
                prev_btn,
                next_btn,
                too_redacted_btn,
                push_to_source_btn,
                useless_btn,
                create_source_link,
            ],
            show_progress=False,
        )

        next_btn.click(
            timed_page_load("/unsorted-files", _next_unsorted_file, label="next_unsorted_file"),
            inputs=[files_state, current_index_state, can_submit_state],
            outputs=[
                current_index_state,
                current_file_id_state,
                explorer_view_html,
                review_shell,
                file_preview_html,
                file_meta_html,
                file_counter,
                current_action_md,
                prev_btn,
                next_btn,
                too_redacted_btn,
                push_to_source_btn,
                useless_btn,
                create_source_link,
            ],
            show_progress=False,
        )

        too_redacted_btn.click(
            timed_page_load("/unsorted-files", _mark_unsorted_too_redacted, label="mark_unsorted_too_redacted"),
            inputs=[current_file_id_state, current_index_state],
            outputs=[
                action_status,
                files_state,
                current_index_state,
                current_file_id_state,
                explorer_view_html,
                review_shell,
                file_preview_html,
                file_meta_html,
                file_counter,
                current_action_md,
                prev_btn,
                next_btn,
                too_redacted_btn,
                push_to_source_btn,
                useless_btn,
                create_source_link,
            ],
            show_progress=False,
        )

        useless_btn.click(
            timed_page_load("/unsorted-files", _mark_unsorted_useless, label="mark_unsorted_useless"),
            inputs=[current_file_id_state, current_index_state],
            outputs=[
                action_status,
                files_state,
                current_index_state,
                current_file_id_state,
                explorer_view_html,
                review_shell,
                file_preview_html,
                file_meta_html,
                file_counter,
                current_action_md,
                prev_btn,
                next_btn,
                too_redacted_btn,
                push_to_source_btn,
                useless_btn,
                create_source_link,
            ],
            show_progress=False,
        )

        push_to_source_btn.click(
            timed_page_load("/unsorted-files", _open_unsorted_push_modal, label="open_unsorted_push_modal"),
            inputs=[current_file_id_state],
            outputs=[push_modal, push_status, push_source_dropdown, push_note],
            show_progress=False,
        )

        push_cancel_btn.click(
            _cancel_unsorted_push_modal,
            outputs=[push_modal, push_status, push_source_dropdown, push_note],
            show_progress=False,
        )

        push_confirm_btn.click(
            timed_page_load(
                "/unsorted-files",
                _submit_unsorted_push_to_source,
                label="submit_unsorted_push_to_source",
            ),
            inputs=[current_file_id_state, push_source_dropdown, push_note, current_index_state],
            outputs=[
                action_status,
                push_modal,
                push_status,
                push_source_dropdown,
                push_note,
                files_state,
                current_index_state,
                current_file_id_state,
                explorer_view_html,
                review_shell,
                file_preview_html,
                file_meta_html,
                file_counter,
                current_action_md,
                prev_btn,
                next_btn,
                too_redacted_btn,
                push_to_source_btn,
                useless_btn,
                create_source_link,
            ],
            show_progress=False,
        )

        tag_file_btn.click(
            timed_page_load("/unsorted-files", _open_unsorted_tags_modal, label="open_unsorted_tags_modal"),
            inputs=[current_file_id_state],
            outputs=[tags_modal, tags_status, tags_input, tags_editor, tags_note],
            show_progress=False,
        )

        tags_cancel_btn.click(
            _cancel_unsorted_tags_modal,
            outputs=[tags_modal, tags_status, tags_input, tags_editor, tags_note],
            show_progress=False,
        )

        tags_confirm_btn.click(
            timed_page_load(
                "/unsorted-files",
                _submit_unsorted_tags_proposal,
                label="submit_unsorted_tags_proposal",
            ),
            inputs=[current_file_id_state, tags_input, tags_note, current_index_state],
            outputs=[
                action_status,
                tags_modal,
                tags_status,
                tags_input,
                tags_editor,
                tags_note,
                files_state,
                current_index_state,
                current_file_id_state,
                explorer_view_html,
                review_shell,
                file_preview_html,
                file_meta_html,
                file_counter,
                current_action_md,
                prev_btn,
                next_btn,
                too_redacted_btn,
                push_to_source_btn,
                useless_btn,
                create_source_link,
            ],
            show_progress=False,
        )

        upload_open_btn.click(
            _open_unsorted_upload_panel,
            inputs=[is_admin_state],
            outputs=[upload_panel, upload_status],
            show_progress=False,
        )

        upload_cancel_btn.click(
            _close_unsorted_upload_panel,
            outputs=[upload_panel, upload_status, upload_files, upload_folder, upload_origin],
            show_progress=False,
        ).then(
            _reset_unsorted_upload_mode,
            outputs=[upload_mode, upload_files, upload_folder, upload_mode_hint],
            show_progress=False,
        )

        upload_submit_btn.click(
            _start_unsorted_upload,
            outputs=[upload_status, upload_submit_btn, upload_cancel_btn],
            show_progress=False,
        ).then(
            timed_page_load("/unsorted-files", _upload_unsorted_files, label="upload_unsorted_files"),
            inputs=[upload_files, upload_folder, upload_origin, current_file_id_state, current_index_state],
            outputs=[
                upload_status,
                upload_panel,
                upload_files,
                upload_folder,
                upload_origin,
                files_state,
                current_index_state,
                current_file_id_state,
                explorer_view_html,
                review_shell,
                file_preview_html,
                file_meta_html,
                file_counter,
                current_action_md,
                prev_btn,
                next_btn,
                too_redacted_btn,
                push_to_source_btn,
                useless_btn,
                create_source_link,
                upload_submit_btn,
                upload_cancel_btn,
            ],
            show_progress=True,
        )

    return app
