function copyToClipboard(text) {
if (navigator.clipboard && typeof navigator.clipboard.writeText === "function") {
return navigator.clipboard.writeText(text);
}
const ta = document.createElement("textarea");
ta.value = text;
ta.style.position = "fixed";
ta.style.top = "-1000px";
document.body.appendChild(ta);
ta.focus();
ta.select();
document.execCommand("copy");
document.body.removeChild(ta);
return Promise.resolve();
}
 
function pythonStringLiteral(value) {
return String(value).replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}
 
function buildScript(host, port) {
const hostEsc = pythonStringLiteral(host);
const portNum = Number(port);
const portVal = Number.isFinite(portNum) && portNum > 0 ? portNum : 22;
 
return `import paramiko
import socket
import os
import sys
import time
 
HOST = os.environ.get("VPS_HOST", "${hostEsc}")
PORT = int(os.environ.get("VPS_PORT", ${portVal}))
CONNECT_TIMEOUT = 30
 
 
def prompt_credentials():
print("=" * 40)
print("  SSH Login")
print("=" * 40)
username = input("Username (default: root): ").strip() or "root"
password = input("Password: ")
return username, password
 
 
def stream_command(client: paramiko.SSHClient, cmd: str, timeout: int = 600) -> bool:
print(f"\\n{'='*60}")
print(f"[+] Running: {cmd}")
print("=" * 60)
 
transport = client.get_transport()
channel = transport.open_session()
channel.settimeout(timeout)
channel.get_pty()
channel.exec_command(cmd)
 
buffer = b""
while True:
if channel.recv_ready():
chunk = channel.recv(4096)
if chunk:
buffer += chunk
while b"\\n" in buffer:
line, buffer = buffer.split(b"\\n", 1)
print(line.decode(errors="replace"))
elif channel.exit_status_ready():
while channel.recv_ready():
chunk = channel.recv(4096)
if chunk:
buffer += chunk
break
else:
time.sleep(0.1)
 
if buffer:
print(buffer.decode(errors="replace"))
 
exit_code = channel.recv_exit_status()
if exit_code != 0:
print(f"[!] Command exited with code {exit_code}")
return False
 
print(f"[✓] Done (exit code 0)")
return True
 
 
def main():
username, password = prompt_credentials()
 
client = paramiko.SSHClient()
try:
print(f"\\n[*] Connecting to {HOST}:{PORT} as '{username}'...")
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
client.connect(
HOST,
port=PORT,
username=username,
password=password,
timeout=CONNECT_TIMEOUT,
allow_agent=False,
look_for_keys=False,
)
print("[+] Connected!\\n")
 
commands = [
"apt-get update && apt-get upgrade -y",
"apt-get install -y curl git sudo",
"bash <(curl -Ls https://github.com/Gozargah/Marzban-installer/raw/master/install.sh)",
]
 
for cmd in commands:
if not stream_command(client, cmd):
print("\\n[✗] A command failed. Stopping.")
sys.exit(1)
 
print("\\n" + "=" * 60)
print("[✓] Marzban installation complete.")
print("\\nNext steps:")
print("  1. Create admin:   marzban cli admin create")
print("  2. Open panel:     http://<your-ip>:8000/dashboard")
print("=" * 60)
 
except paramiko.AuthenticationException:
print("[✗] Authentication failed. Wrong username or password.")
sys.exit(1)
except paramiko.SSHException as e:
print(f"[✗] SSH error: {e}")
sys.exit(1)
except (socket.timeout, TimeoutError):
print(f"[✗] Timed out after {CONNECT_TIMEOUT}s.")
sys.exit(1)
except OSError as e:
print(f"[✗] Network error: {e}")
sys.exit(1)
finally:
client.close()
print("\\n[*] Connection closed.")
 
 
if __name__ == "__main__":
main()
`;
}
 
document.addEventListener("DOMContentLoaded", () => {
const hostEl = document.getElementById("host");
const portEl = document.getElementById("port");
const btnGenerate = document.getElementById("btnGenerate");
const btnCopy = document.getElementById("btnCopy");
const btnDownload = document.getElementById("btnDownload");
const scriptBox = document.getElementById("scriptBox");
 
function setFromQuery() {
const url = new URL(window.location.href);
const host = url.searchParams.get("host");
const port = url.searchParams.get("port");
hostEl.value = host || hostEl.value || "213.142.132.5";
portEl.value = port || portEl.value || "22";
}
 
function generate() {
const host = String(hostEl.value || "").trim() || "213.142.132.5";
const port = String(portEl.value || "").trim() || "22";
scriptBox.value = buildScript(host, port);
}
 
btnGenerate.addEventListener("click", () => {
generate();
});
 
btnCopy.addEventListener("click", async () => {
if (!scriptBox.value) generate();
await copyToClipboard(scriptBox.value);
const prev = btnCopy.textContent;
btnCopy.textContent = "Copied";
setTimeout(() => {
btnCopy.textContent = prev;
}, 900);
});
 
btnDownload.addEventListener("click", () => {
if (!scriptBox.value) generate();
const blob = new Blob([scriptBox.value], { type: "text/x-python;charset=utf-8" });
const url = URL.createObjectURL(blob);
const a = document.createElement("a");
a.href = url;
a.download = "install_marzban_no_proxy.py";
document.body.appendChild(a);
a.click();
document.body.removeChild(a);
setTimeout(() => URL.revokeObjectURL(url), 5000);
});
 
setFromQuery();
generate();
});
 
