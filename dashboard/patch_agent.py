#!/usr/bin/env python3
"""
patch_agent.py - wire the Alerts / Agent tab into the observability demo. Idempotent, keeps .bak-agent copies.

    python3 patch_agent.py /path/to/dashboard            # the folder with app.py and static/

  app.py        mounts the agent router (agent.mount(app, CFG))
  index.html    adds <script src="/static/agent.js">
  config.json   adds an "agent" block if there is none (put your Claude API key there, or export ANTHROPIC_API_KEY)
agent.py and static/agent.js are copied next to it first.
"""
import json
import shutil
import sys
from pathlib import Path

here = Path(__file__).resolve().parent
dash = Path(sys.argv[1] if len(sys.argv) > 1 else ".").resolve()
app_py, index, cfg = dash / "app.py", dash / "static" / "index.html", dash / "config.json"
for f in (app_py, index):
    if not f.exists():
        sys.exit(f"not found: {f}")
ANCHOR = 'app.mount("/static", StaticFiles('
MOUNT = 'try:\n    import agent\n    agent.mount(app, CFG)\nexcept Exception as _e:  # noqa: BLE001\n    print("agent disabled:", _e)\n\n'


def backup(p: Path):
    b = p.with_name(p.name + ".bak-agent")
    if not b.exists():
        shutil.copy2(p, b)


src = app_py.read_text()
if "agent.mount(" in src:
    print("app.py already patched")
else:
    if src.count(ANCHOR) != 1:
        sys.exit("app.py: could not find the static mount line exactly once; nothing was changed")
    backup(app_py)
    app_py.write_text(src.replace(ANCHOR, MOUNT + ANCHOR))
    print("app.py patched")

html = index.read_text()
tag = '<script src="/static/agent.js"></script>'
if tag in html:
    print("index.html already patched")
else:
    if html.count("</body>") != 1:
        sys.exit("index.html: </body> not found once")
    backup(index)
    index.write_text(html.replace("</body>", tag + "\n</body>"))
    print("index.html patched")

if cfg.exists():
    c = json.loads(cfg.read_text())
    if "agent" in c:
        print("config.json already has an agent block")
    else:
        backup(cfg)
        c["agent"] = {"model": "claude-sonnet-5-5", "api_key": "", "ch_user": "agent_user", "ch_password": "agent_demo_pw"}
        cfg.write_text(json.dumps(c, indent=2) + "\n")
        print('config.json: added an "agent" block. Set api_key (or export ANTHROPIC_API_KEY for obs-dash).')
else:
    print("no config.json here; add an \"agent\" block yourself (see README)")

(dash / "static").mkdir(exist_ok=True)
for src_f, dst_f in ((here / "agent.py", dash / "agent.py"), (here / "static" / "agent.js", dash / "static" / "agent.js")):
    if not dst_f.exists() or not src_f.samefile(dst_f):
        shutil.copy2(src_f, dst_f)
print("copied agent.py, static/agent.js\nnext: sql/agent.sql, sql/agent_user.sh, set the API key, restart the dashboard")
