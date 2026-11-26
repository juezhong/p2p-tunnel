//! 用户可见输出层：统一管理提示符、清行重绘与未提交输入镜像。
//!
//! 事件处理与命令回显都通过 [`Output`] 打印，不再直接写 stdout。
//! writer 可注入（测试用内存缓冲），只有 `tty` 为真时才发 ANSI 清行序列；
//! 管道/重定向下只输出普通换行，脚本输出不被控制字符污染。

use std::io::Write;

/// 回到行首并清掉整行（提示符 + 用户已经敲了一半的命令）。
const CLEAR_LINE: &str = "\r\x1b[K";

/// 用户可见输出层。
pub struct Output<W: Write> {
    writer: W,
    tty: bool,
    /// 屏幕上当前行是否处于提示符状态（`> ` 或 `> <未提交输入>`）。
    prompt_shown: bool,
    /// 提示符出现之后是否打印过用户可见行。
    wrote_since_prompt: bool,
    /// 未提交的输入镜像；tty 下重绘时原样恢复。
    input: String,
    /// 当前提示符；随交互状态切换（`> `、`INVITE> `、`[NAT 打洞]> ` 等）。
    prompt: String,
}

impl<W: Write> Output<W> {
    pub fn new(writer: W, tty: bool) -> Self {
        Self {
            writer,
            tty,
            prompt_shown: false,
            wrote_since_prompt: false,
            input: String::new(),
            prompt: "> ".to_string(),
        }
    }

    /// 打印一行用户可见文本。
    ///
    /// tty 下若当前停在提示符行，先清掉提示符与未提交输入，打印完再把
    /// `> ` 和输入原样恢复；非 tty 下只写普通换行。
    pub fn line(&mut self, text: &str) {
        if self.tty && self.prompt_shown {
            self.write(CLEAR_LINE);
            self.write(text);
            self.write("\r\n");
            let prompt = self.prompt.clone();
            self.write(&prompt);
            self.write_mirror();
            self.wrote_since_prompt = false;
        } else {
            self.write(text);
            // tty 下必须写 CRLF，否则终端只下移不回到行首（阶梯状输出）。
            self.write(if self.tty { "\r\n" } else { "\n" });
            self.prompt_shown = false;
            self.wrote_since_prompt = true;
        }
        self.flush();
    }

    /// 打印多行文本块（帮助等），逐行走 [`Output::line`] 以保证 tty 换行正确。
    pub fn block(&mut self, text: &str) {
        for line in text.split('\n') {
            self.line(line);
        }
    }

    /// 打印提示符；没有新输出时不重复打印（空回车不叠加 `> `）。
    pub fn prompt(&mut self) {
        if self.prompt_shown && !self.wrote_since_prompt {
            return;
        }
        let prompt = self.prompt.clone();
        self.write(&prompt);
        self.write_mirror();
        self.prompt_shown = true;
        self.wrote_since_prompt = false;
        self.flush();
    }

    /// 更新未提交输入镜像；tty 下立即重绘成 `> <输入>`。
    ///
    /// 非 tty 不发任何控制序列，也不回显输入。
    pub fn set_input(&mut self, input: &str) {
        if self.input == input {
            return;
        }
        self.input.clear();
        self.input.push_str(input);
        if self.tty && self.prompt_shown {
            self.write(CLEAR_LINE);
            let prompt = self.prompt.clone();
            self.write(&prompt);
            self.write_mirror();
            self.flush();
            self.wrote_since_prompt = false;
        }
    }

    /// 切换提示符；tty 下若正停在提示符行，立即清行重绘成新提示符。
    pub fn set_prompt(&mut self, prompt: &str) {
        if self.prompt == prompt {
            return;
        }
        self.prompt.clear();
        self.prompt.push_str(prompt);
        if self.tty && self.prompt_shown {
            self.write(CLEAR_LINE);
            let prompt = self.prompt.clone();
            self.write(&prompt);
            self.write_mirror();
            self.flush();
            self.wrote_since_prompt = false;
        }
    }

    /// 用户按下回车：tty 下把这一行留在屏幕上，并结束提示符状态。
    pub fn submit_line(&mut self) {
        if self.tty && self.prompt_shown {
            self.write("\r\n");
            self.flush();
        }
        self.prompt_shown = false;
        self.wrote_since_prompt = true;
        self.input.clear();
    }

    /// 退出前收尾提示符：tty 下清掉这一行，避免 `> ` 挂在告别语前面。
    ///
    /// 非 tty 保持原样（现有管道输出形态不变）。
    pub fn end_prompt(&mut self) {
        if self.tty && self.prompt_shown {
            self.write(CLEAR_LINE);
            self.flush();
        }
        self.prompt_shown = false;
    }

    pub fn is_tty(&self) -> bool {
        self.tty
    }

    /// 取出底层 writer（测试断言用）。
    #[cfg(test)]
    pub fn into_writer(self) -> W {
        self.writer
    }

    fn write_mirror(&mut self) {
        let input = self.input.clone();
        self.write(&input);
    }

    fn write(&mut self, text: &str) {
        // 写失败（管道提前关闭）只忽略，绝不让 UI 路径 panic。
        let _ = self.writer.write_all(text.as_bytes());
    }

    fn flush(&mut self) {
        let _ = self.writer.flush();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    fn rendered(out: Output<Vec<u8>>) -> String {
        String::from_utf8(out.into_writer()).expect("输出应为 UTF-8")
    }

    #[test]
    fn non_tty_writes_plain_lines_without_control_sequences() {
        let mut out = Output::new(Vec::new(), false);
        out.prompt();
        out.line("链路: 未连接");
        out.prompt();
        let text = rendered(out);
        assert_eq!(text, "> 链路: 未连接\n> ");
        assert!(!text.contains('\x1b'), "非 tty 不应有 ANSI 序列：{text:?}");
        assert!(!text.contains('\r'), "非 tty 不应有回车符：{text:?}");
    }

    #[test]
    fn empty_enter_does_not_stack_prompts() {
        let mut out = Output::new(Vec::new(), false);
        out.prompt();
        // 连续空回车：没有新输出，不重复打印提示符。
        out.prompt();
        out.prompt();
        assert_eq!(rendered(out), "> ");
    }

    #[test]
    fn prompt_reappears_after_output() {
        let mut out = Output::new(Vec::new(), false);
        out.prompt();
        out.line("[!] 未知命令：q（输入 help 查看帮助）");
        out.prompt();
        out.prompt();
        assert_eq!(rendered(out), "> [!] 未知命令：q（输入 help 查看帮助）\n> ");
    }

    #[test]
    fn tty_redraws_prompt_and_restores_partial_input() {
        let mut out = Output::new(Vec::new(), true);
        out.prompt();
        out.set_input("add 22");
        out.line("[对端] 远程主动断开了 2211");
        let text = rendered(out);
        assert_eq!(
            text,
            "> \r\x1b[K> add 22\r\x1b[K[对端] 远程主动断开了 2211\r\n> add 22"
        );
    }

    #[test]
    fn tty_keeps_echoing_input_as_it_is_typed() {
        let mut out = Output::new(Vec::new(), true);
        out.prompt();
        out.set_input("a");
        out.set_input("ab");
        out.set_input("abc");
        let text = rendered(out);
        assert!(text.ends_with("\r\x1b[K> abc"), "{text:?}");
        // 每次重绘都以清行开始，不叠加旧内容。
        assert_eq!(text.matches("\r\x1b[K> ").count(), 3, "{text:?}");
    }

    #[test]
    fn set_prompt_redraws_and_keeps_partial_input() {
        let mut out = Output::new(Vec::new(), true);
        out.prompt();
        out.set_input("P2PR-");
        out.set_prompt("INVITE> ");
        let text = rendered(out);
        assert!(text.ends_with("\r\x1b[KINVITE> P2PR-"), "{text:?}");
    }

    #[test]
    fn non_tty_prompt_change_applies_to_next_prompt() {
        let mut out = Output::new(Vec::new(), false);
        out.prompt();
        out.line("粘贴对方的 INVITE：");
        out.set_prompt("INVITE> ");
        out.prompt();
        assert_eq!(rendered(out), "> 粘贴对方的 INVITE：\nINVITE> ");
    }

    #[test]
    fn tty_submit_keeps_the_typed_command_in_scrollback() {
        let mut out = Output::new(Vec::new(), true);
        out.prompt();
        out.set_input("exit");
        out.submit_line();
        out.end_prompt();
        out.line("已退出。");
        let text = rendered(out);
        assert!(text.ends_with("\r\n已退出。\r\n"), "{text:?}");
        assert!(text.contains("> exit\r\n"), "{text:?}");
    }

    #[test]
    fn write_errors_do_not_panic() {
        struct Failing;
        impl Write for Failing {
            fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
                Err(io::Error::other("管道已关闭"))
            }
            fn flush(&mut self) -> io::Result<()> {
                Err(io::Error::other("管道已关闭"))
            }
        }
        let mut out = Output::new(Failing, true);
        out.prompt();
        out.set_input("add 2211 22");
        out.line("[+] 监听中");
        out.submit_line();
        out.end_prompt();
        out.prompt();
    }
}
