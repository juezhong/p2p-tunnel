//! stdin 输入：非 tty 行走有界通道；tty 下逐字符读取，保留未提交输入。
//!
//! 逐字符读取是输出层「清行重绘」的前提：只有知道用户敲了一半什么，
//! 事件输出后才能把 `> <未提交命令>` 原样恢复。行编辑只保留最小集合
//! （退格、Ctrl-U、Ctrl-W），其余控制序列忽略。

use std::io::{self, BufRead, Read};
use tokio::sync::mpsc;

/// stdin 读线程交给主循环的输入事件。
pub enum InputEvent {
    /// 非 tty：读到的完整一行（不含换行）。
    Line(String),
    /// tty：一个原始字节。
    Byte(u8),
    /// stdin 关闭。
    Eof,
}

/// 把 stdin 行送入有界通道；通道满时阻塞调用线程（背压）。
pub fn forward_lines<R: BufRead>(reader: R, lines: &mpsc::Sender<InputEvent>) {
    for line in reader.lines() {
        match line {
            Ok(line) => {
                if lines.blocking_send(InputEvent::Line(line)).is_err() {
                    return;
                }
            }
            Err(_) => break,
        }
    }
    let _ = lines.blocking_send(InputEvent::Eof);
}

/// tty 下逐字节读取 stdin（配合 [`RawMode`] 的非规范模式）。
#[cfg(unix)]
pub fn spawn_tty_reader(tx: mpsc::Sender<InputEvent>) {
    std::thread::spawn(move || {
        let mut stdin = io::stdin().lock();
        let mut byte = [0u8; 1];
        loop {
            match stdin.read(&mut byte) {
                Ok(0) => {
                    let _ = tx.blocking_send(InputEvent::Eof);
                    return;
                }
                Ok(_) => {
                    if tx.blocking_send(InputEvent::Byte(byte[0])).is_err() {
                        return;
                    }
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(_) => {
                    let _ = tx.blocking_send(InputEvent::Eof);
                    return;
                }
            }
        }
    });
}

/// 非 unix 没有接管终端输入（见 [`RawMode`]）；此函数不会被调用，保留签名。
#[cfg(not(unix))]
pub fn spawn_tty_reader(tx: mpsc::Sender<InputEvent>) {
    drop(tx);
}

/// 关闭终端的规范模式与回显，改由输出层自己回显和重绘。
///
/// 关闭 `ISIG`：Ctrl-C 作为字节交给 [`LineEditor`] 处理，避免终端停在
/// 非规范模式时被 Ctrl-Z 挂起；外部 `kill -INT` 仍由 tokio 的信号处理接管。
/// Drop 时恢复原设置。
#[cfg(unix)]
pub struct RawMode {
    original: libc::termios,
}

#[cfg(unix)]
impl RawMode {
    pub fn enable() -> Option<Self> {
        // SAFETY: 只操作 stdin 的 termios，缓冲区布局由 libc 提供。
        unsafe {
            let mut current: libc::termios = std::mem::zeroed();
            if libc::tcgetattr(libc::STDIN_FILENO, &mut current) != 0 {
                return None;
            }
            let original = current;
            current.c_lflag &= !(libc::ICANON | libc::ECHO | libc::ISIG);
            current.c_cc[libc::VMIN] = 1;
            current.c_cc[libc::VTIME] = 0;
            if libc::tcsetattr(libc::STDIN_FILENO, libc::TCSADRAIN, &current) != 0 {
                return None;
            }
            Some(Self { original })
        }
    }
}

#[cfg(unix)]
impl Drop for RawMode {
    fn drop(&mut self) {
        // SAFETY: 恢复 enable() 时保存的同一个 termios。
        unsafe {
            libc::tcsetattr(libc::STDIN_FILENO, libc::TCSADRAIN, &self.original);
        }
    }
}

/// 非 unix 不接管终端：`enable` 返回 `None`，走普通行读取。
#[cfg(not(unix))]
pub struct RawMode;

#[cfg(not(unix))]
impl RawMode {
    pub fn enable() -> Option<Self> {
        None
    }
}

/// [`LineEditor::feed`] 的解析结果。
#[derive(Debug, PartialEq, Eq)]
pub enum EditAction {
    /// 只吞掉一个字节（控制序列、未完成的 UTF-8）。
    None,
    /// 输入行变了，需要重绘提示符。
    Redraw,
    /// 用户按了回车：主循环取出整行后清空。
    Submit,
    /// Ctrl-C：按 quit 处理。
    Interrupt,
    /// Ctrl-D 且当前行为空：EOF。
    Eof,
}

#[derive(Default)]
enum Escape {
    #[default]
    None,
    /// 刚读到 ESC，等下一个字节判断序列类型。
    Esc,
    /// 正在吞 CSI/SS3 序列，直到终止字节。
    Sequence,
}

/// tty 行编辑状态机：可脱离终端单测。
#[derive(Default)]
pub struct LineEditor {
    line: String,
    utf8: Vec<u8>,
    escape: Escape,
    /// 上一个字节是 CR：吞掉紧随其后的 LF，避免 CRLF 触发两次提交。
    last_cr: bool,
}

impl LineEditor {
    pub fn new() -> Self {
        Self::default()
    }

    /// 当前未提交的输入。
    pub fn text(&self) -> &str {
        &self.line
    }

    /// 取出整行并清空（回车提交时调用）。
    pub fn take_line(&mut self) -> String {
        self.utf8.clear();
        std::mem::take(&mut self.line)
    }

    pub fn feed(&mut self, byte: u8) -> EditAction {
        match self.escape {
            Escape::Esc => {
                if byte == b'[' || byte == b'O' {
                    self.escape = Escape::Sequence;
                } else {
                    self.escape = Escape::None;
                }
                return EditAction::None;
            }
            Escape::Sequence => {
                // CSI/SS3 的终止字节：0x40..=0x7e。
                if (0x40..=0x7e).contains(&byte) {
                    self.escape = Escape::None;
                }
                return EditAction::None;
            }
            Escape::None => {}
        }

        // CRLF：CR 提交后，紧随的 LF 不再提交一次空行。
        let was_cr = self.last_cr;
        self.last_cr = byte == b'\r';
        if byte == b'\n' && was_cr {
            return EditAction::None;
        }

        match byte {
            b'\r' | b'\n' => {
                self.utf8.clear();
                EditAction::Submit
            }
            0x1b => {
                self.escape = Escape::Esc;
                EditAction::None
            }
            0x03 => EditAction::Interrupt,
            0x04 => {
                if self.line.is_empty() && self.utf8.is_empty() {
                    EditAction::Eof
                } else {
                    EditAction::None
                }
            }
            0x7f | 0x08 => {
                self.utf8.clear();
                if self.line.pop().is_some() {
                    EditAction::Redraw
                } else {
                    EditAction::None
                }
            }
            0x15 => {
                self.utf8.clear();
                if self.line.is_empty() {
                    EditAction::None
                } else {
                    self.line.clear();
                    EditAction::Redraw
                }
            }
            0x17 => {
                self.utf8.clear();
                let trimmed = self.line.trim_end().len();
                if trimmed == 0 {
                    return EditAction::None;
                }
                let keep = self.line[..trimmed]
                    .rfind(char::is_whitespace)
                    .map_or(0, |index| index + 1);
                self.line.truncate(keep);
                EditAction::Redraw
            }
            0x00..=0x1f => EditAction::None,
            0x20..=0x7e => {
                // 有未完成的 UTF-8 时遇到 ASCII：丢弃残缺序列。
                self.utf8.clear();
                self.line.push(byte as char);
                EditAction::Redraw
            }
            _ => self.feed_utf8(byte),
        }
    }

    fn feed_utf8(&mut self, byte: u8) -> EditAction {
        self.utf8.push(byte);
        match std::str::from_utf8(&self.utf8) {
            Ok(text) => {
                self.line.push_str(text);
                self.utf8.clear();
                EditAction::Redraw
            }
            Err(error) if error.error_len().is_none() => EditAction::None,
            Err(_) => {
                // 非法字节序列：丢弃，不让它污染命令行。
                self.utf8.clear();
                EditAction::None
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn type_str(editor: &mut LineEditor, text: &str) {
        for byte in text.bytes() {
            editor.feed(byte);
        }
    }

    #[test]
    fn typing_then_enter_submits_one_line() {
        let mut editor = LineEditor::new();
        type_str(&mut editor, "add 2211 22");
        assert_eq!(editor.text(), "add 2211 22");
        assert_eq!(editor.feed(b'\r'), EditAction::Submit);
        assert_eq!(editor.take_line(), "add 2211 22");
        assert_eq!(editor.text(), "");
    }

    #[test]
    fn crlf_submits_only_once() {
        let mut editor = LineEditor::new();
        type_str(&mut editor, "quit");
        assert_eq!(editor.feed(b'\r'), EditAction::Submit);
        assert_eq!(editor.feed(b'\n'), EditAction::None);
    }

    #[test]
    fn backspace_and_ctrl_u_edit_the_line() {
        let mut editor = LineEditor::new();
        type_str(&mut editor, "add");
        assert_eq!(editor.feed(0x7f), EditAction::Redraw);
        assert_eq!(editor.text(), "ad");
        assert_eq!(editor.feed(0x15), EditAction::Redraw);
        assert_eq!(editor.text(), "");
    }

    #[test]
    fn ctrl_w_removes_the_last_word() {
        let mut editor = LineEditor::new();
        type_str(&mut editor, "add 2211 22");
        assert_eq!(editor.feed(0x17), EditAction::Redraw);
        assert_eq!(editor.text(), "add 2211 ");
    }

    #[test]
    fn control_sequences_are_swallowed() {
        let mut editor = LineEditor::new();
        type_str(&mut editor, "ab");
        // 上箭头：ESC [ A
        for byte in [0x1b, b'[', b'A'] {
            assert_eq!(editor.feed(byte), EditAction::None);
        }
        assert_eq!(editor.text(), "ab");
    }

    #[test]
    fn utf8_input_is_buffered_until_complete() {
        let mut editor = LineEditor::new();
        let bytes = "中".as_bytes();
        assert_eq!(editor.feed(bytes[0]), EditAction::None);
        assert_eq!(editor.feed(bytes[1]), EditAction::None);
        assert_eq!(editor.feed(bytes[2]), EditAction::Redraw);
        assert_eq!(editor.text(), "中");
    }

    #[test]
    fn ctrl_c_interrupts_and_ctrl_d_only_exits_empty_line() {
        let mut editor = LineEditor::new();
        assert_eq!(editor.feed(0x04), EditAction::Eof);
        type_str(&mut editor, "x");
        assert_eq!(editor.feed(0x04), EditAction::None);
        assert_eq!(editor.feed(0x03), EditAction::Interrupt);
    }

    #[test]
    fn forward_lines_sends_lines_then_eof_with_backpressure() {
        let (tx, mut rx) = mpsc::channel::<InputEvent>(2);
        let input: String = (0..10_000).map(|index| format!("line-{index}\n")).collect();
        let writer = std::thread::spawn(move || forward_lines(std::io::Cursor::new(input), &tx));

        // 只有 2 个名额：生产者必须阻塞，而不是把一万行全读进内存。
        std::thread::sleep(std::time::Duration::from_millis(100));
        assert!(!writer.is_finished(), "有界通道应在满时阻塞生产者");

        let mut count = 0usize;
        while let Some(event) = rx.blocking_recv() {
            match event {
                InputEvent::Line(_) => count += 1,
                InputEvent::Eof => break,
                InputEvent::Byte(_) => unreachable!("非 tty 不应出现字节事件"),
            }
        }
        writer.join().expect("写线程结束");
        assert_eq!(count, 10_000);
    }
}
