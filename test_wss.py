import asyncio
import websockets
import json
import os
from aiohttp import web

# --- CONFIGURAZIONE CREDENZIALI ---
USER_KEY = os.environ.get("ETORO_PRIVATE_KEY", "TUA_USER_KEY")
API_KEY = os.environ.get("ETORO_PUBLIC_KEY", "TUA_API_KEY")

# URL suggerito per la Demo (verificare su api-portal.etoro.com)
WSS_URL = os.environ.get("WSS_URL", "wss://public-api.etoro.com/v1/trading/ws")

# --- WEB SERVER PER RENDER (HEALTH CHECK) ---
async def handle_health_check(request):
    """Risponde al ping del Google Script per tenere sveglio Render."""
    return web.Response(text="Bot Cavia is alive and listening")

async def start_web_server():
    """Configura e avvia il server HTTP sulla porta di Render."""
    app = web.Application()
    app.router.add_get('/', handle_health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    
    # Render assegna dinamicamente una porta tramite la variabile d'ambiente PORT
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    print(f"🚀 Health Check server attivo sulla porta {port}")

# --- LOGICA WEBSOCKET ETORO ---
async def etoro_websocket_listener():
    print(f"🔄 Tentativo di connessione a {WSS_URL}...")
    
    try:
        async with websockets.connect(WSS_URL) as websocket:
            print("✅ Connessione stabilita. Esecuzione Handshake di Autenticazione...")
            
            # Autenticazione come da specifiche tecniche eToro
            auth_payload = {
                "operation": "Authenticate",
                "data": {
                    "userKey": USER_KEY,
                    "apiKey": API_KEY
                }
            }
            
            await websocket.send(json.dumps(auth_payload))
            print("📤 Payload di autenticazione inviato.")
            
            auth_response = await websocket.recv()
            print(f"📥 Risposta Autenticazione: {auth_response}")
            
            # Sottoscrizione al topic per notifiche su ordini e posizioni
            sub_payload = {
                "operation": "Subscribe",
                "data": {
                    "topic": "Transaction Updates"
                }
            }
            await websocket.send(json.dumps(sub_payload))
            print("📤 Richiesta di sottoscrizione inviata.")
            
            print("🎧 In ascolto per eventi (es: Trading.OrderForCloseMultiple.Update)...")
            
            while True:
                messaggio = await websocket.recv()
                print(f"\n🔔 [NUOVO EVENTO RICEVUTO]")
                try:
                    dati_json = json.loads(messaggio)
                    # Qui vedremo i campi: OrderID, StatusID, ExecutedUnits, EndRate
                    print(json.dumps(dati_json, indent=4))
                except json.JSONDecodeError:
                    print(messaggio)

    except websockets.exceptions.ConnectionClosed as e:
        print(f"❌ Connessione WebSocket chiusa: {e.code}, {e.reason}")
    except Exception as e:
        print(f"⚠️ Errore imprevisto: {e}")

# --- ENTRY POINT CON DEBUG AVANZATO ---
async def main():
    print("🎬 Inizializzazione applicazione...")
    try:
        # Avvia il server web e il listener WebSocket in parallelo
        await asyncio.gather(
            start_web_server(),
            etoro_websocket_listener()
        )
    except Exception as e:
        print(f"💥 Errore fatale nel ciclo principale: {e}")
    finally:
        # Impedisce l'uscita immediata per leggere i log
        print("⏳ Lo script sta per terminare tra 30 secondi...")
        await asyncio.sleep(30)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"🚨 Errore critico all'avvio: {e}")
