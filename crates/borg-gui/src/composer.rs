use std::ops::Range;

use gpui::{
    App, Bounds, ClipboardItem, Context, CursorStyle, Element, ElementId, ElementInputHandler,
    Entity, EntityInputHandler, FocusHandle, Focusable, GlobalElementId, LayoutId, MouseButton,
    MouseDownEvent, MouseMoveEvent, MouseUpEvent, PaintQuad, Pixels, Point, SharedString, Style,
    TextAlign, TextRun, UTF16Selection, UnderlineStyle, Window, WrappedLine, actions, div, fill,
    hsla, point, prelude::*, px, relative, rgb, rgba, size,
};
use unicode_segmentation::UnicodeSegmentation;

use borg_ui::palette;

fn mask_secret(content: &str) -> String {
    content.graphemes(true).map(|_| '*').collect()
}

fn masked_offset(content: &str, offset: usize) -> usize {
    content
        .grapheme_indices(true)
        .take_while(|(start, _)| *start < offset)
        .count()
}

actions!(
    composer,
    [
        Backspace,
        Delete,
        Left,
        Right,
        SelectLeft,
        SelectRight,
        SelectAll,
        Home,
        End,
        HistoryPrevious,
        HistoryNext,
        Paste,
        Cut,
        Copy,
        Newline,
        Submit
    ]
);

pub struct Submitted(pub String);
pub struct PastedImage(pub gpui::Image);

pub struct Composer {
    focus: FocusHandle,
    content: SharedString,
    selected: Range<usize>,
    reversed: bool,
    marked: Option<Range<usize>>,
    last_layout: Vec<(usize, WrappedLine)>,
    last_bounds: Option<Bounds<Pixels>>,
    selecting: bool,
    secret: bool,
    history: Vec<String>,
    history_index: Option<usize>,
    history_draft: SharedString,
}

impl Composer {
    pub fn new(cx: &mut Context<Self>) -> Self {
        Self {
            focus: cx.focus_handle(),
            content: "".into(),
            selected: 0..0,
            reversed: false,
            marked: None,
            last_layout: Vec::new(),
            last_bounds: None,
            selecting: false,
            secret: false,
            history: Vec::new(),
            history_index: None,
            history_draft: "".into(),
        }
    }

    pub fn set_secret(&mut self, secret: bool, cx: &mut Context<Self>) {
        if self.secret != secret {
            self.secret = secret;
            cx.notify();
        }
    }

    pub fn append_text(&mut self, text: &str, cx: &mut Context<Self>) {
        let separator = if self.content.is_empty()
            || self.content.chars().last().is_some_and(char::is_whitespace)
            || text.chars().next().is_some_and(char::is_whitespace)
        {
            ""
        } else {
            " "
        };
        self.content = format!("{}{separator}{text}", self.content).into();
        self.selected = self.content.len()..self.content.len();
        self.history_index = None;
        cx.notify();
    }

    pub fn append_recalled(&mut self, text: &str, cx: &mut Context<Self>) {
        let separator = if self.content.is_empty() { "" } else { "\n\n" };
        self.content = format!("{}{separator}{text}", self.content).into();
        self.selected = self.content.len()..self.content.len();
        self.marked = None;
        self.history_index = None;
        cx.notify();
    }

    pub fn set_text(&mut self, text: impl Into<SharedString>, cx: &mut Context<Self>) {
        self.content = text.into();
        self.selected = self.content.len()..self.content.len();
        self.marked = None;
        self.history_index = None;
        cx.notify();
    }

    pub fn set_history(&mut self, history: &[String]) {
        if self.history != history {
            self.history = history.to_vec();
            self.history_index = None;
        }
    }

    fn cursor(&self) -> usize {
        if self.reversed {
            self.selected.start
        } else {
            self.selected.end
        }
    }
    fn move_to(&mut self, offset: usize, cx: &mut Context<Self>) {
        self.selected = offset..offset;
        cx.notify();
    }
    fn select_to(&mut self, offset: usize, cx: &mut Context<Self>) {
        if self.reversed {
            self.selected.start = offset
        } else {
            self.selected.end = offset
        }
        if self.selected.end < self.selected.start {
            self.reversed = !self.reversed;
            self.selected = self.selected.end..self.selected.start;
        }
        cx.notify();
    }
    fn previous(&self, offset: usize) -> usize {
        self.content
            .grapheme_indices(true)
            .rev()
            .find_map(|(i, _)| (i < offset).then_some(i))
            .unwrap_or(0)
    }
    fn next(&self, offset: usize) -> usize {
        self.content
            .grapheme_indices(true)
            .find_map(|(i, _)| (i > offset).then_some(i))
            .unwrap_or(self.content.len())
    }
    fn left(&mut self, _: &Left, _: &mut Window, cx: &mut Context<Self>) {
        self.move_to(
            if self.selected.is_empty() {
                self.previous(self.cursor())
            } else {
                self.selected.start
            },
            cx,
        );
    }
    fn right(&mut self, _: &Right, _: &mut Window, cx: &mut Context<Self>) {
        self.move_to(
            if self.selected.is_empty() {
                self.next(self.cursor())
            } else {
                self.selected.end
            },
            cx,
        );
    }
    fn select_left(&mut self, _: &SelectLeft, _: &mut Window, cx: &mut Context<Self>) {
        self.select_to(self.previous(self.cursor()), cx);
    }
    fn select_right(&mut self, _: &SelectRight, _: &mut Window, cx: &mut Context<Self>) {
        self.select_to(self.next(self.cursor()), cx);
    }
    fn select_all(&mut self, _: &SelectAll, _: &mut Window, cx: &mut Context<Self>) {
        self.move_to(0, cx);
        self.select_to(self.content.len(), cx);
    }
    fn home(&mut self, _: &Home, _: &mut Window, cx: &mut Context<Self>) {
        self.move_to(0, cx);
    }
    fn end(&mut self, _: &End, _: &mut Window, cx: &mut Context<Self>) {
        self.move_to(self.content.len(), cx);
    }
    fn history_previous(&mut self, _: &HistoryPrevious, _: &mut Window, cx: &mut Context<Self>) {
        let Some(index) = self
            .history_index
            .map(|index| index.saturating_sub(1))
            .or_else(|| {
                self.history_draft = self.content.clone();
                self.history.len().checked_sub(1)
            })
        else {
            return;
        };
        self.history_index = Some(index);
        self.content = self.history[index].clone().into();
        self.selected = self.content.len()..self.content.len();
        self.marked = None;
        cx.notify();
    }
    fn history_next(&mut self, _: &HistoryNext, _: &mut Window, cx: &mut Context<Self>) {
        let Some(index) = self.history_index else {
            return;
        };
        if index + 1 < self.history.len() {
            self.history_index = Some(index + 1);
            self.content = self.history[index + 1].clone().into();
        } else {
            self.history_index = None;
            self.content = self.history_draft.clone();
        }
        self.selected = self.content.len()..self.content.len();
        self.marked = None;
        cx.notify();
    }
    fn backspace(&mut self, _: &Backspace, window: &mut Window, cx: &mut Context<Self>) {
        if self.selected.is_empty() {
            self.select_to(self.previous(self.cursor()), cx);
        }
        self.replace_text_in_range(None, "", window, cx);
    }
    fn delete(&mut self, _: &Delete, window: &mut Window, cx: &mut Context<Self>) {
        if self.selected.is_empty() {
            self.select_to(self.next(self.cursor()), cx);
        }
        self.replace_text_in_range(None, "", window, cx);
    }
    fn paste(&mut self, _: &Paste, window: &mut Window, cx: &mut Context<Self>) {
        if let Some(item) = cx.read_from_clipboard() {
            if let Some(text) = item.text() {
                self.replace_text_in_range(None, &text, window, cx);
            } else if let Some(image) = item.into_entries().find_map(|entry| match entry {
                gpui::ClipboardEntry::Image(image) => Some(image),
                gpui::ClipboardEntry::String(_) => None,
            }) {
                cx.emit(PastedImage(image));
            }
        }
    }
    fn copy(&mut self, _: &Copy, _: &mut Window, cx: &mut Context<Self>) {
        if !self.selected.is_empty() {
            cx.write_to_clipboard(ClipboardItem::new_string(
                self.content[self.selected.clone()].to_string(),
            ));
        }
    }
    fn cut(&mut self, _: &Cut, window: &mut Window, cx: &mut Context<Self>) {
        self.copy(&Copy, window, cx);
        if !self.selected.is_empty() {
            self.replace_text_in_range(None, "", window, cx);
        }
    }
    fn submit(&mut self, _: &Submit, _: &mut Window, cx: &mut Context<Self>) {
        let text = self.content.trim().to_string();
        if text.is_empty() {
            return;
        }
        self.content = "".into();
        self.selected = 0..0;
        self.marked = None;
        self.history_index = None;
        self.history_draft = "".into();
        cx.emit(Submitted(text));
        cx.notify();
    }

    fn newline(&mut self, _: &Newline, window: &mut Window, cx: &mut Context<Self>) {
        self.replace_text_in_range(None, "\n", window, cx);
    }

    fn position_for_index(&self, index: usize) -> Option<Point<Pixels>> {
        let bounds = self.last_bounds?;
        let line_height = px(24.);
        let mut top = bounds.top();
        let (start, line) = self
            .last_layout
            .iter()
            .rev()
            .find(|(start, _)| *start <= index)?;
        for (_, preceding) in self.last_layout.iter().take_while(|item| item.0 < *start) {
            top += preceding.size(line_height).height;
        }
        let local = index.saturating_sub(*start).min(line.len());
        let position = line.position_for_index(local, line_height)?;
        Some(point(bounds.left() + position.x, top + position.y))
    }

    fn index_at(&self, position: Point<Pixels>) -> usize {
        let Some(bounds) = self.last_bounds else {
            return 0;
        };
        if position.y < bounds.top() {
            0
        } else if position.y > bounds.bottom() {
            self.content.len()
        } else {
            let line_height = px(24.);
            let mut top = bounds.top();
            for (start, line) in &self.last_layout {
                let height = line.size(line_height).height;
                if position.y <= top + height {
                    let local = line
                        .closest_index_for_position(
                            point(position.x - bounds.left(), position.y - top),
                            line_height,
                        )
                        .unwrap_or_else(|index| index);
                    return (start + local).min(self.content.len());
                }
                top += height;
            }
            self.content.len()
        }
    }
    fn mouse_down(&mut self, event: &MouseDownEvent, _: &mut Window, cx: &mut Context<Self>) {
        self.selecting = true;
        let i = self.index_at(event.position);
        if event.modifiers.shift {
            self.select_to(i, cx);
        } else {
            self.move_to(i, cx);
        }
    }
    fn mouse_up(&mut self, _: &MouseUpEvent, _: &mut Window, _: &mut Context<Self>) {
        self.selecting = false;
    }
    fn mouse_move(&mut self, event: &MouseMoveEvent, _: &mut Window, cx: &mut Context<Self>) {
        if self.selecting {
            self.select_to(self.index_at(event.position), cx);
        }
    }
    fn byte_offset_from_utf16(&self, offset: usize) -> usize {
        self.content
            .chars()
            .take_while(|_| true)
            .scan((0, 0), |state, ch| {
                if state.1 >= offset {
                    return None;
                }
                state.0 += ch.len_utf8();
                state.1 += ch.len_utf16();
                Some(state.0)
            })
            .last()
            .unwrap_or(0)
    }
    fn to_utf16(&self, offset: usize) -> usize {
        self.content[..offset].encode_utf16().count()
    }
    fn range_from_utf16(&self, range: &Range<usize>) -> Range<usize> {
        self.byte_offset_from_utf16(range.start)..self.byte_offset_from_utf16(range.end)
    }
    fn range_to_utf16(&self, range: &Range<usize>) -> Range<usize> {
        self.to_utf16(range.start)..self.to_utf16(range.end)
    }
}

impl gpui::EventEmitter<Submitted> for Composer {}
impl gpui::EventEmitter<PastedImage> for Composer {}
impl Focusable for Composer {
    fn focus_handle(&self, _: &App) -> FocusHandle {
        self.focus.clone()
    }
}

impl EntityInputHandler for Composer {
    fn text_for_range(
        &mut self,
        range: Range<usize>,
        actual: &mut Option<Range<usize>>,
        _: &mut Window,
        _: &mut Context<Self>,
    ) -> Option<String> {
        let range = self.range_from_utf16(&range);
        actual.replace(self.range_to_utf16(&range));
        Some(self.content[range].to_string())
    }
    fn selected_text_range(
        &mut self,
        _: bool,
        _: &mut Window,
        _: &mut Context<Self>,
    ) -> Option<UTF16Selection> {
        Some(UTF16Selection {
            range: self.range_to_utf16(&self.selected),
            reversed: self.reversed,
        })
    }
    fn marked_text_range(&self, _: &mut Window, _: &mut Context<Self>) -> Option<Range<usize>> {
        self.marked.as_ref().map(|r| self.range_to_utf16(r))
    }
    fn unmark_text(&mut self, _: &mut Window, _: &mut Context<Self>) {
        self.marked = None;
    }
    fn replace_text_in_range(
        &mut self,
        range: Option<Range<usize>>,
        text: &str,
        _: &mut Window,
        cx: &mut Context<Self>,
    ) {
        let range = range
            .as_ref()
            .map(|r| self.range_from_utf16(r))
            .or(self.marked.clone())
            .unwrap_or(self.selected.clone());
        self.content =
            (self.content[..range.start].to_owned() + text + &self.content[range.end..]).into();
        self.history_index = None;
        self.selected = range.start + text.len()..range.start + text.len();
        self.marked = None;
        cx.notify();
    }
    fn replace_and_mark_text_in_range(
        &mut self,
        range: Option<Range<usize>>,
        text: &str,
        selected: Option<Range<usize>>,
        window: &mut Window,
        cx: &mut Context<Self>,
    ) {
        self.replace_text_in_range(range, text, window, cx);
        let end = self.selected.end;
        if !text.is_empty() {
            self.marked = Some(end - text.len()..end);
        }
        if let Some(selected) = selected {
            let start = end - text.len();
            let r = self.range_from_utf16(&selected);
            self.selected = start + r.start..start + r.end;
        }
    }
    fn bounds_for_range(
        &mut self,
        range: Range<usize>,
        bounds: Bounds<Pixels>,
        _: &mut Window,
        _: &mut Context<Self>,
    ) -> Option<Bounds<Pixels>> {
        let range = self.range_from_utf16(&range);
        let start = self.position_for_index(range.start)?;
        let end = self.position_for_index(range.end)?;
        Some(Bounds::from_corners(
            start,
            point(
                end.x.max(start.x + px(1.)),
                (end.y + px(24.)).min(bounds.bottom()),
            ),
        ))
    }
    fn character_index_for_point(
        &mut self,
        point: Point<Pixels>,
        _: &mut Window,
        _: &mut Context<Self>,
    ) -> Option<usize> {
        Some(self.to_utf16(self.index_at(point)))
    }
}

struct ComposerElement {
    input: Entity<Composer>,
}
struct Prepaint {
    lines: Vec<(usize, WrappedLine)>,
    cursor: Option<PaintQuad>,
}
impl IntoElement for ComposerElement {
    type Element = Self;
    fn into_element(self) -> Self {
        self
    }
}
impl Element for ComposerElement {
    type RequestLayoutState = ();
    type PrepaintState = Prepaint;
    fn id(&self) -> Option<ElementId> {
        None
    }
    fn source_location(&self) -> Option<&'static core::panic::Location<'static>> {
        None
    }
    fn request_layout(
        &mut self,
        _: Option<&GlobalElementId>,
        _: Option<&gpui::InspectorElementId>,
        window: &mut Window,
        cx: &mut App,
    ) -> (LayoutId, ()) {
        let mut style = Style::default();
        style.size.width = relative(1.).into();
        style.size.height = px(72.).into();
        (window.request_layout(style, [], cx), ())
    }
    fn prepaint(
        &mut self,
        _: Option<&GlobalElementId>,
        _: Option<&gpui::InspectorElementId>,
        bounds: Bounds<Pixels>,
        _: &mut (),
        window: &mut Window,
        cx: &mut App,
    ) -> Prepaint {
        let input = self.input.read(cx);
        let content = input.content.clone();
        let mut cursor = input.cursor();
        let mut selected = input.selected.clone();
        let mut marked = input.marked.clone();
        if input.secret {
            cursor = masked_offset(&content, cursor);
            selected =
                masked_offset(&content, selected.start)..masked_offset(&content, selected.end);
            marked = marked.map(|range| {
                masked_offset(&content, range.start)..masked_offset(&content, range.end)
            });
        }
        let style = window.text_style();
        let (display, color) = if content.is_empty() {
            (
                SharedString::from("Type a follow-up…"),
                hsla(0., 0., 0.62, 0.65),
            )
        } else if input.secret {
            (mask_secret(&content).into(), style.color)
        } else {
            (content, style.color)
        };
        let base_run = TextRun {
            len: display.len(),
            font: style.font(),
            color,
            background_color: None,
            underline: None,
            strikethrough: None,
        };
        let mut boundaries = vec![0, display.len()];
        if !input.content.is_empty() {
            boundaries.extend([selected.start, selected.end]);
            if let Some(marked) = &marked {
                boundaries.extend([marked.start, marked.end]);
            }
        }
        boundaries.sort_unstable();
        boundaries.dedup();
        let runs = boundaries
            .windows(2)
            .filter_map(|range| {
                let start = range[0];
                let end = range[1];
                (end > start).then(|| TextRun {
                    len: end - start,
                    background_color: (!selected.is_empty()
                        && start < selected.end
                        && end > selected.start)
                        .then(|| rgba(0x4aa3ff44).into()),
                    underline: marked.as_ref().and_then(|marked| {
                        (start < marked.end && end > marked.start).then(|| UnderlineStyle {
                            color: Some(base_run.color),
                            thickness: px(1.),
                            wavy: false,
                        })
                    }),
                    ..base_run.clone()
                })
            })
            .collect::<Vec<_>>();
        let shaped = window
            .text_system()
            .shape_text(
                display.clone(),
                style.font_size.to_pixels(window.rem_size()),
                &runs,
                Some(bounds.size.width),
                Some(3),
            )
            .unwrap_or_default();
        let mut offset = 0;
        let lines = shaped
            .into_iter()
            .map(|line| {
                let start = offset;
                offset += line.len();
                if display.as_bytes().get(offset) == Some(&b'\n') {
                    offset += 1;
                }
                (start, line)
            })
            .collect::<Vec<_>>();
        let cursor = selected.is_empty().then(|| {
            let line_height = px(24.);
            let mut top = bounds.top();
            let mut position = point(bounds.left(), bounds.top());
            if let Some((start, line)) = lines.iter().rev().find(|(start, _)| *start <= cursor) {
                for (_, preceding) in lines.iter().take_while(|item| item.0 < *start) {
                    top += preceding.size(line_height).height;
                }
                if let Some(local) = line
                    .position_for_index(cursor.saturating_sub(*start).min(line.len()), line_height)
                {
                    position = point(bounds.left() + local.x, top + local.y);
                }
            }
            fill(
                Bounds::new(position, size(px(1.5), line_height)),
                rgb(palette::ORANGE),
            )
        });
        Prepaint { lines, cursor }
    }
    fn paint(
        &mut self,
        _: Option<&GlobalElementId>,
        _: Option<&gpui::InspectorElementId>,
        bounds: Bounds<Pixels>,
        _: &mut (),
        state: &mut Prepaint,
        window: &mut Window,
        cx: &mut App,
    ) {
        let focus = self.input.read(cx).focus.clone();
        window.handle_input(
            &focus,
            ElementInputHandler::new(bounds, self.input.clone()),
            cx,
        );
        let line_height = px(24.);
        let mut top = bounds.top();
        for (_, line) in &state.lines {
            line.paint(
                point(bounds.left(), top),
                line_height,
                TextAlign::Left,
                Some(bounds),
                window,
                cx,
            )
            .ok();
            top += line.size(line_height).height;
        }
        if focus.is_focused(window)
            && let Some(q) = state.cursor.take()
        {
            window.paint_quad(q);
        }
        self.input.update(cx, |input, _| {
            input.last_layout = state.lines.clone();
            input.last_bounds = Some(bounds);
        });
    }
}

impl Render for Composer {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        div()
            .key_context("Composer")
            .track_focus(&self.focus)
            .cursor(CursorStyle::IBeam)
            .on_action(cx.listener(Self::backspace))
            .on_action(cx.listener(Self::delete))
            .on_action(cx.listener(Self::left))
            .on_action(cx.listener(Self::right))
            .on_action(cx.listener(Self::select_left))
            .on_action(cx.listener(Self::select_right))
            .on_action(cx.listener(Self::select_all))
            .on_action(cx.listener(Self::home))
            .on_action(cx.listener(Self::end))
            .on_action(cx.listener(Self::history_previous))
            .on_action(cx.listener(Self::history_next))
            .on_action(cx.listener(Self::paste))
            .on_action(cx.listener(Self::cut))
            .on_action(cx.listener(Self::copy))
            .on_action(cx.listener(Self::newline))
            .on_action(cx.listener(Self::submit))
            .on_mouse_down(MouseButton::Left, cx.listener(Self::mouse_down))
            .on_mouse_up(MouseButton::Left, cx.listener(Self::mouse_up))
            .on_mouse_up_out(MouseButton::Left, cx.listener(Self::mouse_up))
            .on_mouse_move(cx.listener(Self::mouse_move))
            .w_full()
            .h(px(72.))
            .line_height(px(24.))
            .text_size(px(14.))
            .child(ComposerElement { input: cx.entity() })
    }
}

#[cfg(test)]
mod tests {
    use super::{mask_secret, masked_offset};

    #[test]
    fn secret_mask_uses_one_cell_per_grapheme() {
        assert_eq!(mask_secret("中文e\u{301}👩‍💻"), "****");
        assert_eq!(masked_offset("中文e\u{301}👩‍💻", "中文e\u{301}".len()), 3);
    }
}
