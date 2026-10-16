
import asyncio
import logging
import os
import secrets
import random
from contextlib import asynccontextmanager

import uvicorn
from aiogram import Bot, Dispatcher
from aiogram.filters import CommandStart
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import HTMLResponse

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("infinity-hub")

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
WEBAPP_URL = (
    os.getenv("WEBAPP_URL", "").strip()
    or os.getenv("RENDER_EXTERNAL_URL", "").strip()
).rstrip("/")
PORT = int(os.getenv("PORT", "8000"))

bot = None
dp = Dispatcher()
rooms = {}
room_lock = asyncio.Lock()

ONLINE_GAMES = {"tic", "connect", "rps"}
GAME_NAMES = {
    "tic": "Tic-Tac-Toe",
    "connect": "Connect Four",
    "rps": "Rock Paper Scissors",
}


def new_state(game):
    if game == "tic":
        return {"board": [""] * 9, "turn": 1, "winner": 0}
    if game == "connect":
        return {"board": [""] * 42, "turn": 1, "winner": 0}
    if game == "rps":
        return {"choices": {}, "winner": 0, "round_done": False}
    raise ValueError("Unsupported online game")


def tic_winner(board):
    lines = [
        (0, 1, 2), (3, 4, 5), (6, 7, 8),
        (0, 3, 6), (1, 4, 7), (2, 5, 8),
        (0, 4, 8), (2, 4, 6),
    ]
    for a, b, c in lines:
        if board[a] and board[a] == board[b] == board[c]:
            return board[a]
    return None


def connect_winner(board, mark):
    for r in range(6):
        for c in range(7):
            for dr, dc in ((0, 1), (1, 0), (1, 1), (1, -1)):
                cells = []
                for k in range(4):
                    rr, cc = r + dr * k, c + dc * k
                    if not (0 <= rr < 6 and 0 <= cc < 7):
                        break
                    cells.append(board[rr * 7 + cc])
                if len(cells) == 4 and all(x == mark for x in cells):
                    return True
    return False


def public_state(room, role):
    game = room["game"]
    state = room["state"]
    data = {
        "type": "state",
        "game": game,
        "role": role,
        "players": len(room["players"]),
        "status": room["status"],
        "state": state,
        "message": room.get("message", ""),
    }
    if game == "rps" and not state["round_done"]:
        data["state"] = {
            "choices": {
                str(k): ("Ready" if v else "")
                for k, v in state["choices"].items()
            },
            "winner": 0,
            "round_done": False,
        }
    return data


async def broadcast(code):
    room = rooms.get(code)
    if not room:
        return
    dead = []
    for ws, role in list(room["players"].items()):
        try:
            await ws.send_json(public_state(room, role))
        except Exception:
            dead.append(ws)
    for ws in dead:
        room["players"].pop(ws, None)


async def apply_move(room, role, move):
    game = room["game"]
    state = room["state"]

    if room["status"] != "playing":
        return "Waiting for the second player."
    if state.get("winner", 0):
        return "This match has already finished."

    if game in ("tic", "connect"):
        if state["turn"] != role:
            return "Wait for your turn."

        if game == "tic":
            index = move.get("index")
            if not isinstance(index, int) or not 0 <= index < 9:
                return "Invalid move."
            if state["board"][index]:
                return "That cell is already occupied."

            state["board"][index] = "X" if role == 1 else "O"
            winner = tic_winner(state["board"])
            if winner:
                state["winner"] = 1 if winner == "X" else 2
                room["status"] = "finished"
                room["message"] = f"Player {state['winner']} wins!"
            elif all(state["board"]):
                room["status"] = "finished"
                room["message"] = "Draw!"
            else:
                state["turn"] = 2 if role == 1 else 1
            return ""

        column = move.get("column")
        if not isinstance(column, int) or not 0 <= column < 7:
            return "Invalid column."

        row = None
        for r in range(5, -1, -1):
            idx = r * 7 + column
            if not state["board"][idx]:
                row = r
                break
        if row is None:
            return "That column is full."

        mark = "R" if role == 1 else "Y"
        state["board"][row * 7 + column] = mark
        if connect_winner(state["board"], mark):
            state["winner"] = role
            room["status"] = "finished"
            room["message"] = f"Player {role} wins!"
        elif all(state["board"]):
            room["status"] = "finished"
            room["message"] = "Draw!"
        else:
            state["turn"] = 2 if role == 1 else 1
        return ""

    if game == "rps":
        if state["round_done"]:
            return "This round has finished."

        choice = move.get("choice")
        if choice not in ("rock", "paper", "scissors"):
            return "Invalid choice."
        if role in state["choices"]:
            return "You have already chosen."

        state["choices"][role] = choice
        if len(state["choices"]) == 2:
            a, b = state["choices"][1], state["choices"][2]
            if a == b:
                state["winner"] = 0
                room["message"] = f"Draw! Both chose {a}."
            elif (a, b) in (
                ("rock", "scissors"),
                ("scissors", "paper"),
                ("paper", "rock"),
            ):
                state["winner"] = 1
                room["message"] = f"Player 1 wins! {a} beats {b}."
            else:
                state["winner"] = 2
                room["message"] = f"Player 2 wins! {b} beats {a}."
            state["round_done"] = True
            room["status"] = "finished"
        else:
            room["message"] = f"Player {role} has chosen. Waiting..."
        return ""

    return "Unsupported game."


@asynccontextmanager
async def lifespan(app):
    global bot
    polling_task = None

    if BOT_TOKEN:
        bot = Bot(BOT_TOKEN)
        polling_task = asyncio.create_task(dp.start_polling(bot))
        log.info("Telegram polling task started")
    else:
        log.warning("BOT_TOKEN missing; web server only")

    yield

    if polling_task:
        polling_task.cancel()
        try:
            await polling_task
        except asyncio.CancelledError:
            pass

    if bot:
        await bot.session.close()


app = FastAPI(lifespan=lifespan)


@app.get("/", response_class=HTMLResponse)
async def home():
    return HTML


@app.head("/")
async def head_home():
    return {}


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "service": "Infinity Game Hub",
        "bot_configured": bool(BOT_TOKEN),
        "webapp_url_configured": bool(WEBAPP_URL),
    }


@app.post("/api/rooms")
async def create_room(payload: dict):
    game = payload.get("game")
    if game not in ONLINE_GAMES:
        raise HTTPException(400, "Online mode is not available for this game.")

    async with room_lock:
        code = secrets.token_hex(3).upper()
        while code in rooms:
            code = secrets.token_hex(3).upper()

        rooms[code] = {
            "game": game,
            "state": new_state(game),
            "players": {},
            "status": "waiting",
            "message": "Waiting for player 2...",
        }

    return {"code": code, "game": game}


@app.websocket("/ws/{code}")
async def online_room(ws: WebSocket, code: str):
    code = code.upper()
    await ws.accept()

    async with room_lock:
        room = rooms.get(code)
        if room is None:
            await ws.send_json({"type": "error", "message": "Room not found."})
            await ws.close()
            return

        if len(room["players"]) >= 2:
            await ws.send_json({"type": "error", "message": "Room is full."})
            await ws.close()
            return

        role = len(room["players"]) + 1
        room["players"][ws] = role
        if len(room["players"]) == 2:
            room["status"] = "playing"
            room["message"] = "Both players connected. Match started!"

    await broadcast(code)

    try:
        while True:
            msg = await ws.receive_json()
            if not isinstance(msg, dict) or msg.get("type") != "move":
                await ws.send_json({
                    "type": "error",
                    "message": "Invalid request.",
                })
                continue

            async with room_lock:
                room = rooms.get(code)
                if not room or ws not in room["players"]:
                    break
                error = await apply_move(room, role, msg)

            if error:
                await ws.send_json({"type": "error", "message": error})
            await broadcast(code)

    except WebSocketDisconnect:
        pass
    except Exception:
        log.exception("Online room error")
    finally:
        async with room_lock:
            room = rooms.get(code)
            if room and ws in room["players"]:
                room["players"].pop(ws, None)
                if not room["players"]:
                    rooms.pop(code, None)
                else:
                    room["status"] = "waiting"
                    room["message"] = "Opponent disconnected. Create a new room to restart."
        await broadcast(code)


@dp.message(CommandStart())
async def start_command(message: Message):
    if not WEBAPP_URL.startswith("https://"):
        await message.answer(
            "WebApp URL is not configured.\n"
            "Please set WEBAPP_URL or check Render's RENDER_EXTERNAL_URL."
        )
        return

    keyboard = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="🎮 OPEN INFINITY GAME HUB",
            web_app=WebAppInfo(url=WEBAPP_URL),
        )
    ]])

    await message.answer(
        "🔥 Welcome to INFINITY GAME HUB!\n\n"
        "Choose your game, challenge a friend, and have fun.",
        reply_markup=keyboard,
    )


HTML = r"""
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,maximum-scale=1,user-scalable=no">
<meta name="theme-color" content="#080b13">
<title>INFINITY GAME HUB</title>
<style>
:root{--bg:#080b13;--panel:#111827;--line:#29354d;--cyan:#5bf4ff;--violet:#a78bfa;--muted:#91a0b8}
*{box-sizing:border-box}
body{margin:0;min-height:100vh;color:#f4f7ff;background:radial-gradient(ellipse at 50% -10%,#25365c 0,#0b1020 45%,#05070b 100%);font-family:system-ui,Arial,sans-serif}
header{text-align:center;padding:27px 16px 18px}
.brand{font-size:11px;font-weight:900;letter-spacing:5px;color:var(--cyan)}
h1{font-size:clamp(28px,8vw,44px);letter-spacing:-1px;margin:10px 0;background:linear-gradient(90deg,#fff,var(--cyan),var(--violet));background-clip:text;color:transparent}
.tag{font-size:11px;letter-spacing:3px;color:var(--muted)}
main{max-width:920px;margin:auto;padding:10px 14px 40px}
.panel{background:#101725eF;border:1px solid var(--line);border-radius:20px;padding:16px;margin-bottom:15px;box-shadow:0 15px 40px #0004}
.label{font-size:10px;font-weight:800;letter-spacing:2px;color:var(--muted);margin-bottom:12px}
.grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:9px}
button{min-height:46px;padding:12px 10px;border-radius:12px;border:1px solid #34415c;background:linear-gradient(135deg,#1a2941,#111724);color:#f6f8ff;font-weight:750;cursor:pointer}
button:active{transform:scale(.98)}
button.primary{background:linear-gradient(115deg,#176e7b,#5743a0);border-color:#65efff}
button:disabled{opacity:.45}
.game-card{text-align:left;min-height:73px}
.game-card span{display:block;font-size:11px;color:#9caec8;font-weight:500;margin-top:5px}
.game{display:none;text-align:center}
.game h2{margin:5px 0 14px}
#status{min-height:24px;color:var(--cyan);font-size:14px}
#board{display:grid;gap:7px;max-width:380px;margin:16px auto}
.cell{aspect-ratio:1;min-height:0;padding:0;font-size:clamp(22px,7vw,38px)}
#result{font-size:20px;color:var(--cyan);min-height:28px;margin:12px 0}
.small{font-size:12px;line-height:1.6;color:#94a3b8}
.hidden{display:none!important}
input{width:100%;padding:12px;background:#080d16;border:1px solid #33415d;color:white;border-radius:10px;margin:7px 0}
footer{text-align:center;color:#6e7a92;font-size:10px;letter-spacing:2px;padding:15px}
@media(min-width:600px){.grid.games{grid-template-columns:repeat(3,minmax(0,1fr))}}
</style>
</head>
<body>
<header>
 <div class="brand">PIZZA AI PRESENTS</div>
 <h1>INFINITY GAME HUB</h1>
 <div class="tag">PLAY • COMPETE • CONQUER</div>
</header>
<main>
 <section class="panel" id="home">
  <div class="label">SELECT YOUR GAME</div>
  <div class="grid games" id="games"></div>
 </section>

 <section class="panel">
  <div class="label">SELECT GAME MODE</div>
  <div class="grid">
   <button onclick="setMode('bot')">🤖 SOLO VS BOT</button>
   <button onclick="setMode('local')">👥 LOCAL 2 PLAYER</button>
   <button class="primary" onclick="setMode('online')">🌐 ONLINE MATCH</button>
   <button onclick="goHome()">⌂ GAME MENU</button>
  </div>
  <div class="small" id="modeInfo">Choose a game first.</div>
 </section>

 <section class="panel game" id="play">
  <div class="label" id="gameLabel">GAME SESSION</div>
  <h2 id="gameTitle"></h2>
  <div id="status"></div>
  <div id="board"></div>
  <div id="result"></div>
  <div id="onlinePanel" class="hidden">
   <div class="label">ONLINE ROOM</div>
   <button class="primary" onclick="createRoom()">＋ CREATE ROOM</button>
   <input id="roomCode" placeholder="Enter your friend's room code">
   <button onclick="joinRoom()">JOIN ROOM</button>
   <div class="small" id="roomInfo"></div>
  </div>
  <div class="grid">
   <button onclick="restart()">↻ RESTART</button>
   <button onclick="goHome()">⌂ GAME MENU</button>
  </div>
 </section>
</main>
<footer>INFINITY GAME HUB • TELEGRAM WEBAPP</footer>
<script>
const games=[
 ["tic","⭕ Tic-Tac-Toe","Strategy"],
 ["rps","✊ Rock Paper Scissors","Quick duel"],
 ["connect","🔴 Connect Four","Classic strategy"],
 ["memory","🧠 Memory Match","Find pairs"],
 ["math","⚡ Math Blitz","Speed maths"],
 ["word","🔤 Word Scramble","Word puzzle"],
 ["nim","💎 Crystal Nim","Take turns"],
 ["number","🎯 Number Duel","Guessing game"],
 ["tap","👆 Tap Rush","15-second challenge"],
 ["reaction","⚡ Reaction Clash","Reaction test"]
];
const $=id=>document.getElementById(id);
let selected="tic", mode="bot", state={}, socket=null, room="", role=0, gameActive=false;
games.forEach(([id,name,desc])=>{
 const b=document.createElement("button");
 b.className="game-card";b.innerHTML=name+"<span>"+desc+"</span>";
 b.onclick=()=>chooseGame(id,name);$("games").appendChild(b);
});
function chooseGame(id,name){selected=id;$("gameTitle").textContent=name;startGame();}
function setMode(m){mode=m;$("modeInfo").textContent="Mode: "+m.toUpperCase();if(selected)startGame();}
function closeSocket(){if(socket){socket.onclose=null;socket.close();socket=null;}room="";role=0;}
function startGame(){
 closeSocket();state={};gameActive=true;
 $("play").style.display="block";$("home").style.display="none";
 $("gameTitle").textContent=(games.find(g=>g[0]===selected)||[])[1]||selected;
 $("result").textContent="";$("board").innerHTML="";
 $("onlinePanel").classList.toggle("hidden",mode!=="online");
 $("roomInfo").textContent="";$("roomCode").value="";
 if(mode==="online"){
  if(!["tic","connect","rps"].includes(selected)){
   $("status").textContent="Online mode is currently available for Tic-Tac-Toe, Connect Four and Rock Paper Scissors.";
   return;
  }
  $("status").textContent="Create a room or join your friend's room.";
  return;
 }
 if(selected==="tic"){state.board=Array(9).fill("");state.turn=1;drawTic();}
 else if(selected==="connect"){state.board=Array(42).fill("");state.turn=1;drawConnect();}
 else if(selected==="rps")drawRPS();
 else if(selected==="memory"){state.cards=[1,2,3,4,5,6,1,2,3,4,5,6].sort(()=>Math.random()-.5);state.open=[];state.matched=[];drawMemory();}
 else if(selected==="math"){state.score=0;state.q=0;newMath();}
 else if(selected==="word")newWord();
 else if(selected==="nim"){state.stones=15;drawNim();}
 else if(selected==="number"){state.target=1+Math.floor(Math.random()*20);state.tries=0;drawNumber();}
 else if(selected==="tap"){state.score=0;state.end=Date.now()+15000;drawTap();}
 else if(selected==="reaction")drawReaction();
}
function finish(msg){$("result").textContent=msg;$("status").textContent="MATCH COMPLETE";}
function drawTic(){
 $("status").textContent="Player "+state.turn+"'s turn";
 $("board").style.gridTemplateColumns="repeat(3,1fr)";$("board").innerHTML="";
 state.board.forEach((v,i)=>{let b=document.createElement("button");b.className="cell";b.textContent=v;b.onclick=()=>ticMove(i);b.disabled=!!v||!!$("result").textContent;$("board").appendChild(b);});
}
function ticWin(b){return [[0,1,2],[3,4,5],[6,7,8],[0,3,6],[1,4,7],[2,5,8],[0,4,8],[2,4,6]].some(a=>b[a[0]]&&b[a[0]]===b[a[1]]&&b[a[1]]===b[a[2]]);}
function ticMove(i){
 if(mode==="online"){sendMove({index:i});return;}
 if(state.board[i]||$("result").textContent)return;
 state.board[i]=state.turn===1?"X":"O";
 if(ticWin(state.board)){drawTic();return finish("Player "+state.turn+" wins!");}
 if(state.board.every(Boolean)){drawTic();return finish("Draw!");}
 state.turn=state.turn===1?2:1;
 if(mode==="bot"&&state.turn===2){
  const free=state.board.map((v,i)=>v?null:i).filter(v=>v!==null);
  if(free.length)state.board[free[Math.floor(Math.random()*free.length)]]="O";
  if(ticWin(state.board)){drawTic();return finish("Bot wins!");}
  if(state.board.every(Boolean)){drawTic();return finish("Draw!");}
  state.turn=1;
 }
 drawTic();
}
function drawConnect(){
 $("status").textContent="Player "+state.turn+"'s turn";
 $("board").style.gridTemplateColumns="repeat(7,1fr)";$("board").innerHTML="";
 state.board.forEach((v,i)=>{let b=document.createElement("button");b.className="cell";b.style.fontSize="18px";b.textContent=v==="R"?"🔴":v==="Y"?"🟡":"·";b.onclick=()=>connectMove(i%7);b.disabled=!!$("result").textContent;$("board").appendChild(b);});
}
function cWin(b,p){for(let r=0;r<6;r++)for(let c=0;c<7;c++)for(let [dr,dc] of [[0,1],[1,0],[1,1],[1,-1]]){let a=[];for(let k=0;k<4;k++){let rr=r+dr*k,cc=c+dc*k;if(rr<0||rr>=6||cc<0||cc>=7)break;a.push(b[rr*7+cc]);}if(a.length===4&&a.every(x=>x===p))return true;}return false;}
function putConnect(col,p){for(let r=5;r>=0;r--){let i=r*7+col;if(!state.board[i]){state.board[i]=p;return true;}}return false;}
function connectMove(col){
 if(mode==="online"){sendMove({column:col});return;}
 if($("result").textContent||!putConnect(col,state.turn===1?"R":"Y"))return;
 if(cWin(state.board,state.turn===1?"R":"Y")){drawConnect();return finish("Player "+state.turn+" wins!");}
 if(state.board.every(Boolean)){drawConnect();return finish("Draw!");}
 state.turn=state.turn===1?2:1;
 if(mode==="bot"&&state.turn===2){
  const legal=[];for(let c=0;c<7;c++)if(!state.board[c])legal.push(c);
  if(legal.length){let col2=legal[Math.floor(Math.random()*legal.length)];putConnect(col2,"Y");}
  if(cWin(state.board,"Y")){drawConnect();return finish("Bot wins!");}
  if(state.board.every(Boolean)){drawConnect();return finish("Draw!");}
  state.turn=1;
 }
 drawConnect();
}
function drawChoices(items,fn){
 $("board").style.gridTemplateColumns="repeat(2,minmax(0,1fr))";$("board").innerHTML="";
 items.forEach(([label,value])=>{let b=document.createElement("button");b.textContent=label;b.onclick=()=>fn(value);$("board").appendChild(b);});
}
function drawRPS(){
 $("status").textContent=mode==="local"?"Player 1 chooses first":"Choose your move";
 const opts=[["✊ Rock","rock"],["✋ Paper","paper"],["✌️ Scissors","scissors"]];
 drawChoices(opts,choice=>{
  if(mode==="online"){sendMove({choice});return;}
  if(mode==="bot"){
   const bot=["rock","paper","scissors"][Math.floor(Math.random()*3)];
   return finish(rpsResult(choice,bot,"You","Bot"));
  }
  if(!state.first){state.first=choice;$("status").textContent="Player 1 chose. Player 2, choose now.";return;}
  finish(rpsResult(state.first,choice,"Player 1","Player 2"));
 });
}
function rpsResult(a,b,na,nb){if(a===b)return "Draw! Both chose "+a+".";let win=(a==="rock"&&b==="scissors")||(a==="scissors"&&b==="paper")||(a==="paper"&&b==="rock");return (win?na:nb)+" wins!";}
function drawMemory(){
 $("status").textContent="Matched pairs: "+(state.matched.length/2);
 $("board").style.gridTemplateColumns="repeat(4,1fr)";$("board").innerHTML="";
 state.cards.forEach((v,i)=>{let b=document.createElement("button");b.className="cell";b.textContent=state.matched.includes(i)||state.open.includes(i)?v:"?";b.onclick=()=>memoryMove(i);$("board").appendChild(b);});
}
function memoryMove(i){
 if(state.open.includes(i)||state.matched.includes(i)||state.open.length>=2)return;
 state.open.push(i);drawMemory();
 if(state.open.length===2){let [a,b]=state.open;if(state.cards[a]===state.cards[b]){state.matched.push(a,b);state.open=[];drawMemory();if(state.matched.length===state.cards.length)finish("Perfect memory!");}else setTimeout(()=>{if(selected==="memory"){state.open=[];drawMemory();}},650);}
}
function newMath(){
 state.a=1+Math.floor(Math.random()*20);state.b=1+Math.floor(Math.random()*20);state.ans=state.a+state.b;
 $("gameTitle").textContent=state.a+" + "+state.b+" = ?";
 $("status").textContent="Score: "+state.score+" • Question "+(state.q+1);
 let vals=[state.ans,state.ans-1,state.ans+2,state.ans+4].sort(()=>Math.random()-.5);
 drawChoices(vals.map(String).map(v=>[v,v]),v=>{if(Number(v)===state.ans)state.score++;state.q++;if(state.q>=10)finish("Your score: "+state.score+"/10");else newMath();});
}
const words=["PLANET","GAMING","PLAYER","LEGEND","VICTORY","INFINITY","THUNDER","CRYSTAL"];
function newWord(){
 state.word=words[Math.floor(Math.random()*words.length)];
 state.scramble=state.word.split("").sort(()=>Math.random()-.5).join("");
 $("status").textContent="Unscramble: "+state.scramble;
 $("board").innerHTML='<input id="wordAnswer" placeholder="Type your answer"><button onclick="checkWord()">SUBMIT ANSWER</button>';
}
function checkWord(){let a=$("wordAnswer").value.trim().toUpperCase();finish(a===state.word?"Correct!":"Answer: "+state.word);}
function drawNim(){
 $("status").textContent="Stones remaining: "+state.stones;
 drawChoices([["Take 1","1"],["Take 2","2"],["Take 3","3"]],v=>{
  let n=Number(v);if(n>state.stones)return;state.stones-=n;
  if(state.stones<=0)return finish("You win!");
  if(mode==="bot"){let bot=Math.min(state.stones,1+Math.floor(Math.random()*3));state.stones-=bot;if(state.stones<=0)return finish("Bot wins!");}
  else {state.turn=state.turn===1?2:1;}
  drawNim();
 });
}
function drawNumber(){
 $("status").textContent="Guess 1–20 • Attempts: "+state.tries;
 $("board").innerHTML='<input id="numberGuess" type="number" min="1" max="20" placeholder="Enter a number"><button onclick="guessNumber()">GUESS</button>';
}
function guessNumber(){
 let n=Number($("numberGuess").value);if(!Number.isInteger(n)||n<1||n>20){$("status").textContent="Enter a number from 1 to 20.";return;}
 state.tries++;if(n===state.target)return finish("Correct in "+state.tries+" tries!");
 $("status").textContent=n<state.target?"Too low! • Attempts: "+state.tries:"Too high! • Attempts: "+state.tries;
}
function drawTap(){
 $("status").textContent="Tap as fast as possible for 15 seconds!";
 let b=document.createElement("button");b.textContent="TAP!";b.style.fontSize="34px";b.style.minHeight="140px";
 b.onclick=()=>{if(Date.now()>=state.end)return finish("Final score: "+state.score);state.score++;$("status").textContent="Score: "+state.score;};
 $("board").innerHTML="";$("board").appendChild(b);
 setTimeout(()=>{if(selected==="tap"&&gameActive)finish("Final score: "+state.score);},15000);
}
function drawReaction(){
 $("status").textContent="Wait until the button turns green.";
 let b=document.createElement("button");b.textContent="WAIT...";b.disabled=true;b.style.minHeight="120px";b.style.fontSize="25px";
 $("board").innerHTML="";$("board").appendChild(b);state.ready=false;
 b.onclick=()=>{if(!state.ready)return finish("Too early! Restart to try again.");finish("Reaction time: "+(Date.now()-state.started)+" ms");};
 setTimeout(()=>{if(selected!=="reaction"||!gameActive)return;state.ready=true;state.started=Date.now();b.disabled=false;b.textContent="🟢 TAP NOW!";b.style.background="linear-gradient(120deg,#087a52,#21bf83)";},1800+Math.random()*3000);
}
async function createRoom(){
 try{
  const res=await fetch("/api/rooms",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({game:selected})});
  const data=await res.json();if(!res.ok)throw new Error(data.detail||"Room creation failed");
  room=data.code;$("roomCode").value=room;$("roomInfo").textContent="Room code: "+room+" • Share it with your friend.";
  connectRoom(room);
 }catch(e){$("roomInfo").textContent="Could not create room: "+e.message;}
}
function joinRoom(){const code=$("roomCode").value.trim().toUpperCase();if(!code){$("roomInfo").textContent="Enter a room code.";return;}room=code;connectRoom(code);}
function connectRoom(code){
 closeSocket();room=code;
 const scheme=location.protocol==="https:"?"wss://":"ws://";
 socket=new WebSocket(scheme+location.host+"/ws/"+encodeURIComponent(code));
 socket.onopen=()=>{$("roomInfo").textContent="Connected. Waiting for room state...";};
 socket.onmessage=e=>{
  const msg=JSON.parse(e.data);
  if(msg.type==="error"){$("roomInfo").textContent=msg.message;return;}
  if(msg.type==="state"){role=msg.role;renderOnline(msg);}
 };
 socket.onerror=()=>{$("roomInfo").textContent="Connection error. Check the room code and try again.";};
 socket.onclose=()=>{};
}
function sendMove(move){if(!socket||socket.readyState!==WebSocket.OPEN){$("status").textContent="Not connected to a room.";return;}socket.send(JSON.stringify({type:"move",...move}));}
function renderOnline(msg){
 const s=msg.state;state=s;
 $("roomInfo").textContent="Room "+room+" • You are Player "+role+" • Players: "+msg.players+"/2";
 $("status").textContent=msg.message||"Online match";
 $("result").textContent=msg.status==="finished"?(msg.message||"Match finished"):"";
 if(msg.game==="tic"){
  $("board").style.gridTemplateColumns="repeat(3,1fr)";$("board").innerHTML="";
  s.board.forEach((v,i)=>{let b=document.createElement("button");b.className="cell";b.textContent=v;b.disabled=msg.status!=="playing"||!!v||s.turn!==role;b.onclick=()=>sendMove({index:i});$("board").appendChild(b);});
  if(msg.status==="playing")$("status").textContent=s.turn===role?"Your turn":"Opponent's turn";
 }else if(msg.game==="connect"){
  $("board").style.gridTemplateColumns="repeat(7,1fr)";$("board").innerHTML="";
  s.board.forEach((v,i)=>{let b=document.createElement("button");b.className="cell";b.style.fontSize="18px";b.textContent=v==="R"?"🔴":v==="Y"?"🟡":"·";b.disabled=msg.status!=="playing"||s.turn!==role||!!$("result").textContent;b.onclick=()=>sendMove({column:i%7});$("board").appendChild(b);});
  if(msg.status==="playing")$("status").textContent=s.turn===role?"Your turn":"Opponent's turn";
 }else if(msg.game==="rps"){
  if(msg.status==="finished"){$("board").innerHTML="";$("result").textContent=msg.message;return;}
  const already=s.choices&&s.choices[String(role)];
  drawChoices([["✊ Rock","rock"],["✋ Paper","paper"],["✌️ Scissors","scissors"]],choice=>sendMove({choice}));
  if(already){$("board").querySelectorAll("button").forEach(b=>b.disabled=true);$("status").textContent="You chose. Waiting for your opponent...";}
 }
}
function restart(){if(mode==="online"){closeSocket();$("status").textContent="Create a new room or join an existing room.";return;}startGame();}
function goHome(){gameActive=false;closeSocket();$("play").style.display="none";$("home").style.display="block";$("result").textContent="";}
</script>
</body>
</html>
"""


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
