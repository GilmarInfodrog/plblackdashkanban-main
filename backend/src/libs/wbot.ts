import * as Sentry from "@sentry/node";
import makeWASocket, {
  WASocket,
  AuthenticationState,
  DisconnectReason,
  fetchLatestBaileysVersion,
  AuthenticationCreds,
  makeInMemoryStore,
  isJidBroadcast,
  BinaryNode,
  getBinaryNodeChild
} from "@whiskeysockets/baileys";
import { randomBytes } from "crypto";
import makeWALegacySocket from "@whiskeysockets/baileys";
import P from "pino";

import Whatsapp from "../models/Whatsapp";
import { logger } from "../utils/logger";
import MAIN_LOGGER from "@whiskeysockets/baileys/lib/Utils/logger";
import authState from "../helpers/authState";
import { Boom } from "@hapi/boom";
import AppError from "../errors/AppError";
import { getIO } from "./socket";
import { Store } from "./store";
import { StartWhatsAppSession } from "../services/WbotServices/StartWhatsAppSession";
import DeleteBaileysService from "../services/BaileysServices/DeleteBaileysService";

const loggerBaileys = MAIN_LOGGER.child({});
loggerBaileys.level = "error";

type Session = WASocket & {
  id?: number;
  store?: Store;
};

const sessions: Session[] = [];

const retriesQrCodeMap = new Map<number, number>();

export const getWbot = (whatsappId: number): Session => {
  const sessionIndex = sessions.findIndex(s => s.id === whatsappId);

  if (sessionIndex === -1) {
    throw new AppError("ERR_WAPP_NOT_INITIALIZED");
  }
  return sessions[sessionIndex];
};

export const removeWbot = async (
  whatsappId: number,
  isLogout = true
): Promise<void> => {
  try {
    const sessionIndex = sessions.findIndex(s => s.id === whatsappId);
    if (sessionIndex !== -1) {
      if (isLogout) {
        sessions[sessionIndex].logout();
        sessions[sessionIndex].ws.close();
      }

      sessions.splice(sessionIndex, 1);
    }
  } catch (err) {
    logger.error(err);
  }
};

export const initWASocket = async (whatsapp: Whatsapp): Promise<Session> => {
  return new Promise(async (resolve, reject) => {
    try {
      (async () => {
        const io = getIO();

        const whatsappUpdate = await Whatsapp.findOne({
          where: { id: whatsapp.id }
        });

        if (!whatsappUpdate) return;

        const { id, name, provider } = whatsappUpdate;

        const { version, isLatest } = await fetchLatestBaileysVersion();
        const isLegacy = provider === "stable" ? true : false;

        logger.info(`using WA v${version.join(".")}, isLatest: ${isLatest}`);
        logger.info(`isLegacy: ${isLegacy}`);
        logger.info(`Starting session ${name}`);
        let retriesQrCode = 0;

        let wsocket: Session = null;
        const store = makeInMemoryStore({
          logger: loggerBaileys
        });

        const { state, saveState } = await authState(whatsapp);

        wsocket = makeWASocket({
          version,
          logger: loggerBaileys,
          printQRInTerminal: false,
          auth: state as AuthenticationState,
          generateHighQualityLinkPreview: false,
          shouldIgnoreJid: jid => isJidBroadcast(jid),
          browser: ["Ubuntu", "Chrome", "22.04.4"],
          patchMessageBeforeSending: (message) => {
            const requiresPatch = !!(
                message.buttonsMessage ||
                // || message.templateMessage
                message.listMessage
            );
            if (requiresPatch) {
                message = {
                    viewOnceMessage: {
                        message: {
                            messageContextInfo: {
                                deviceListMetadataVersion: 2,
                                deviceListMetadata: {},
                            },
                            ...message,
                        },
                    },
                };
            }

            return message;
        },
        })

        // Desde jul/2026 o WhatsApp envia "companion_reg_refresh" durante o
        // pareamento pedindo um novo advSecretKey. O Baileys 6.x ignora o aviso
        // e o QR continua com a chave antiga ("Não foi possível conectar o
        // dispositivo"). Mesma logica do PR #2765 do Baileys.
        let lastQrCode = "";
        const withCurrentAdvSecret = (qrCode: string): string => {
          const parts = qrCode.split(",");
          parts[3] = wsocket.authState.creds.advSecretKey;
          return parts.join(",");
        };

        wsocket.ws.on("CB:notification", async (node: BinaryNode) => {
          const isRegRefresh =
            node.attrs?.type === "companion_reg_refresh" ||
            !!getBinaryNodeChild(node, "companion_reg_refresh") ||
            !!getBinaryNodeChild(node, "pair-device-rotate-qr");

          // depois de pareado a chave nao pode mudar
          if (!isRegRefresh || !wsocket || wsocket.authState.creds.me) return;

          const advSecretKey = randomBytes(32).toString("base64");
          Object.assign(wsocket.authState.creds, { advSecretKey });
          wsocket.ev.emit("creds.update", { advSecretKey });
          logger.info(`Session ${name} advSecretKey renovado (companion_reg_refresh)`);

          if (!lastQrCode) return;
          lastQrCode = withCurrentAdvSecret(lastQrCode);
          await whatsapp.update({ qrcode: lastQrCode });
          io.emit(`company-${whatsapp.companyId}-whatsappSession`, {
            action: "update",
            session: whatsapp
          });
        });

        wsocket.ev.on(
          "connection.update",
          async ({ connection, lastDisconnect, qr }) => {
            logger.info(
              `Socket  ${name} Connection Update ${connection || ""} ${lastDisconnect || ""
              }`
            );

            if (connection === "close") {
              if ((lastDisconnect?.error as Boom)?.output?.statusCode === 403) {
                await whatsapp.update({ status: "PENDING", session: "" });
                await DeleteBaileysService(whatsapp.id);
                io.emit(`company-${whatsapp.companyId}-whatsappSession`, {
                  action: "update",
                  session: whatsapp
                });
                removeWbot(id, false);
              }
              if (
                (lastDisconnect?.error as Boom)?.output?.statusCode !==
                DisconnectReason.loggedOut
              ) {
                removeWbot(id, false);
                setTimeout(
                  () => StartWhatsAppSession(whatsapp, whatsapp.companyId),
                  2000
                );
              } else {
                await whatsapp.update({ status: "PENDING", session: "" });
                await DeleteBaileysService(whatsapp.id);
                io.emit(`company-${whatsapp.companyId}-whatsappSession`, {
                  action: "update",
                  session: whatsapp
                });
                removeWbot(id, false);
                setTimeout(
                  () => StartWhatsAppSession(whatsapp, whatsapp.companyId),
                  2000
                );
              }
            }

            if (connection === "open") {
              await whatsapp.update({
                status: "CONNECTED",
                qrcode: "",
                retries: 0
              });

              io.emit(`company-${whatsapp.companyId}-whatsappSession`, {
                action: "update",
                session: whatsapp
              });

              const sessionIndex = sessions.findIndex(
                s => s.id === whatsapp.id
              );
              if (sessionIndex === -1) {
                wsocket.id = whatsapp.id;
                sessions.push(wsocket);
              }

              resolve(wsocket);
            }

            if (qr !== undefined) {
              if (retriesQrCodeMap.get(id) && retriesQrCodeMap.get(id) >= 3) {
                await whatsappUpdate.update({
                  status: "DISCONNECTED",
                  qrcode: ""
                });
                await DeleteBaileysService(whatsappUpdate.id);
                io.emit("whatsappSession", {
                  action: "update",
                  session: whatsappUpdate
                });
                wsocket.ev.removeAllListeners("connection.update");
                wsocket.ws.close();
                wsocket = null;
                retriesQrCodeMap.delete(id);
              } else {
                logger.info(`Session QRCode Generate ${name}`);
                retriesQrCodeMap.set(id, (retriesQrCode += 1));

                // o Baileys reaproveita a chave antiga nos QRs seguintes
                lastQrCode = withCurrentAdvSecret(qr);

                await whatsapp.update({
                  qrcode: lastQrCode,
                  status: "qrcode",
                  retries: 0
                });
                const sessionIndex = sessions.findIndex(
                  s => s.id === whatsapp.id
                );

                if (sessionIndex === -1) {
                  wsocket.id = whatsapp.id;
                  sessions.push(wsocket);
                }

                io.emit(`company-${whatsapp.companyId}-whatsappSession`, {
                  action: "update",
                  session: whatsapp
                });
              }
            }
          }
        );
        wsocket.ev.on("creds.update", saveState);

        store.bind(wsocket.ev);
      })();
    } catch (error) {
      Sentry.captureException(error);
      console.log(error);
      reject(error);
    }
  });
};
