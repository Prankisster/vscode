import * as vscode from "vscode";
import {
  CloseAction,
  ErrorAction,
  ErrorHandlerResult,
  LanguageClient,
  LanguageClientOptions,
  Message,
  State,
  Trace,
} from "vscode-languageclient/node";
import { Logger } from "../logging";
import { MultiWebSocketServerManager } from "./multiWebSocketServerManager";
import { ServerOptionsProvider } from "./serverOptionsProvider";

const logger = new Logger("flinkSql.languageClient");

// Track original positions to single-line positions mapping
const positionMappings = new Map<string, Map<number, vscode.Position>>();

// Add this near the top after the positionMappings declaration
let enableExtraCompletionDebug = false;

/** Initialize the FlinkSQL language client and connect to the language server websocket
 * @returns A promise that resolves to the language client, or null if initialization failed
 * Prerequisites:
 * - User is authenticated with CCloud
 * - User has selected a compute pool
 */
export async function initializeLanguageClient(
  serverManager: MultiWebSocketServerManager,
  onWebSocketDisconnect: () => void,
): Promise<LanguageClient | null> {
  try {
    logger.info("Initializing FlinkSQL language client");

    const documentSelector = [
      { language: "flinksql" },
      { scheme: "untitled", language: "flinksql" },
      { pattern: "**/*.flink.sql" },
    ];

    // Create the server options provider
    logger.debug("Creating server options provider");
    const serverOptionsProvider = new ServerOptionsProvider(serverManager, documentSelector);

    const clientOptions: LanguageClientOptions = {
      documentSelector,
      middleware: {
        didOpen: (document, next) => {
          logger.debug(`Document opened: ${document.uri.toString()}`);
          return next(document);
        },
        provideCompletionItem: async (document, position, context, token, next) => {
          logger.debug(
            `Providing completion items for ${document.uri.toString()} at position ${position.line}:${position.character}`,
          );

          // Store the original position
          const uri = document.uri.toString();
          const originalPosition = new vscode.Position(position.line, position.character);

          // Calculate the single line position
          const singleLinePosition = convertToSingleLinePosition(document, originalPosition);

          // Store mapping from single line to original position
          let docMappings = positionMappings.get(uri);
          if (!docMappings) {
            docMappings = new Map<number, vscode.Position>();
            positionMappings.set(uri, docMappings);
          }
          docMappings.set(singleLinePosition.character, originalPosition);

          // Extra logging for debugging
          if (enableExtraCompletionDebug) {
            const lineText = document.lineAt(position.line).text;
            const linePrefix = lineText.substring(0, position.character);
            logger.info(`Completion context - Line ${position.line}: "${lineText}"`);
            logger.info(`Prefix: "${linePrefix}"`);
            logger.info(
              `TriggerCharacter: ${context.triggerCharacter || "none"}, TriggerKind: ${context.triggerKind}`,
            );
          }

          logger.debug(
            `Stored mapping from single line pos ${singleLinePosition.character} to ${position.line}:${position.character}`,
          );

          // Call the next handler with the original position - the position conversion happens in sendRequest middleware
          const result: any = await next(document, position, context, token);

          if (result) {
            logger.debug(`Got ${result.items?.length || 0} completion items for ${uri}`);

            // Extra logging for debugging
            if (enableExtraCompletionDebug && result.items) {
              logger.info(
                `Completion items before processing: ${JSON.stringify(result.items.slice(0, 3))}`,
              );
            }

            // Process completion items to fix their ranges
            const items: any = result.items;
            if (items && Array.isArray(items)) {
              items.forEach((element: vscode.CompletionItem) => {
                // The server sends backticks in the filterText for all Resource completions, but vscode languageclient
                // will filter out these items if the completion range doesn't start with a backtick, so we remove them
                if (
                  element.filterText &&
                  element.filterText.startsWith("`") &&
                  element.filterText.endsWith("`")
                ) {
                  element.filterText = element.filterText.substring(
                    1,
                    element.filterText.length - 1,
                  );
                }

                // Fix position in ranges
                if (element.textEdit && element.textEdit.range) {
                  // Adjust the range to use the original line number instead of line 0
                  if (element.textEdit.range.start.line === 0 && position.line > 0) {
                    logger.debug(`Fixing range from line 0 to line ${position.line}`);
                    // Create a new range with the correct line number since range properties are read-only
                    element.textEdit.range = new vscode.Range(
                      new vscode.Position(position.line, element.textEdit.range.start.character),
                      new vscode.Position(position.line, element.textEdit.range.end.character),
                    );
                  }
                } else if (position.line > 0) {
                  // If there's no textEdit, explicitly add one with the correct line number
                  // This ensures completions work properly even if the server doesn't provide a range
                  logger.debug(`Adding missing textEdit for item ${element.label}`);
                  element.textEdit = new vscode.TextEdit(
                    new vscode.Range(
                      new vscode.Position(position.line, position.character),
                      new vscode.Position(position.line, position.character),
                    ),
                    typeof element.insertText === "string"
                      ? element.insertText
                      : typeof element.label === "string"
                        ? element.label
                        : element.label.toString(),
                  );
                }
              });

              // Extra logging for debugging
              if (enableExtraCompletionDebug) {
                logger.info(
                  `Completion items after processing: ${JSON.stringify(result.items.slice(0, 3))}`,
                );
              }
            }
            return result;
          }
          return [];
        },
        sendRequest: (type, params, token, next) => {
          // Log information about the request being sent
          let requestInfo = "Unknown request";
          if (typeof type === "object" && type.method) {
            requestInfo = type.method;
          } else if (typeof type === "string") {
            requestInfo = type;
          }

          logger.debug(`Sending request: ${requestInfo}`);

          // Server does not accept line positions > 0 for completions, so we need to convert them to single-line
          if (
            typeof type === "object" &&
            type.method &&
            type.method === "textDocument/completion"
          ) {
            if (params && (params as any).position && (params as any).textDocument?.uri) {
              const uri = (params as any).textDocument.uri;
              const document = vscode.workspace.textDocuments.find(
                (doc) => doc.uri.toString() === uri,
              );
              if (document) {
                const originalPosition = (params as any).position;
                const singleLinePosition = convertToSingleLinePosition(
                  document,
                  new vscode.Position(originalPosition.line, originalPosition.character),
                );

                logger.debug(
                  `Converted position from ${originalPosition.line}:${originalPosition.character} to 0:${singleLinePosition.character}`,
                );

                // Update the position in the params
                (params as any).position = singleLinePosition;
              }
            }
          }

          return next(type, params, token);
        },
      },
      initializationFailedHandler: (error) => {
        logger.error(`Language server initialization failed: ${error}`);
        return true; // Don't send the user an error, we are handling it
      },
      errorHandler: {
        error: (error: Error, message: Message): ErrorHandlerResult => {
          logger.error(`Language server error: ${message}`);
          return {
            action: ErrorAction.Continue,
            message: `${message ?? error.message}`,
            handled: true, // Don't send the user an error, we are handling it
          };
        },
        closed: () => {
          logger.warn("Language server connection closed by the client's error handler");
          onWebSocketDisconnect();
          return {
            action: CloseAction.Restart,
            handled: true, // Don't send the user an error, we are handling it
          };
        },
      },
      synchronize: {
        // Tell the server about file changes in the workspace
        fileEvents: vscode.workspace.createFileSystemWatcher("**/*.sql"),
      },
    };

    const languageClient = new LanguageClient(
      "confluent.flinksqlLanguageServer",
      "ConfluentFlinkSQL",
      serverOptionsProvider.getServerOptions(),
      clientOptions,
    );

    // Register debug commands only if they don't already exist
    const commandIds = vscode.commands.getCommands(true);

    // Helper function to register a command only if it doesn't exist yet
    const registerCommandSafely = async (id: string, callback: (...args: any[]) => any) => {
      const commands = await commandIds;
      if (!commands.includes(id)) {
        logger.debug(`Registering command: ${id}`);
        return vscode.commands.registerCommand(id, callback);
      } else {
        logger.warn(`Command ${id} already exists, skipping registration`);
        return { dispose: () => {} }; // Return a no-op disposable
      }
    };

    // Register the completion test command with safe registration
    const testCompletionsCmd = await registerCommandSafely(
      "extension.flinkSql.testCompletions",
      async () => {
        if (!languageClient || !languageClient.isRunning()) {
          vscode.window.showErrorMessage("FlinkSQL language client is not running");
          return;
        }

        const editor = vscode.window.activeTextEditor;
        if (!editor) {
          vscode.window.showErrorMessage("No active editor");
          return;
        }

        const document = editor.document;
        if (!vscode.languages.match(documentSelector, document)) {
          vscode.window.showErrorMessage("Not a FlinkSQL document");
          return;
        }

        try {
          logger.info("Running test completions command");
          const position = editor.selection.active;

          // Send a manual completion request to test the functionality
          const completions: any = await languageClient.sendRequest("textDocument/completion", {
            textDocument: { uri: document.uri.toString() },
            position: {
              line: position.line,
              character: position.character,
            },
          });

          if (
            completions &&
            typeof completions === "object" &&
            "items" in completions &&
            Array.isArray(completions.items) &&
            completions.items.length > 0
          ) {
            logger.info(`Received ${completions.items.length} completion items`);
            vscode.window.showInformationMessage(
              `Received ${completions.items.length} completion items`,
            );

            // Show the first few completion items
            const output = vscode.window.createOutputChannel("FlinkSQL Completions");
            output.appendLine(
              `Completion items at position ${position.line}:${position.character}:`,
            );
            completions.items.slice(0, 10).forEach((item: any, index: number) => {
              output.appendLine(`${index + 1}. ${item.label} (${item.kind || "unknown kind"})`);
            });
            output.show();
          } else {
            logger.warn("No completion items received");
            vscode.window.showWarningMessage("No completion items received");
          }
        } catch (error) {
          logger.error(`Error testing completions: ${error}`);
          vscode.window.showErrorMessage(`Error testing completions: ${error}`);
        }
      },
    );

    // Register the debug toggle command with safe registration and unique ID
    const debugToggleCmd = await registerCommandSafely(
      "extension.flinkSql.toggleCompletionDebug",
      () => {
        enableExtraCompletionDebug = !enableExtraCompletionDebug;
        const status = enableExtraCompletionDebug ? "enabled" : "disabled";
        logger.info(`Extra completion debugging ${status}`);
        vscode.window.showInformationMessage(`FlinkSQL extra completion debugging ${status}`);
      },
    );

    // Create a disposable collection for cleanup
    const disposables: vscode.Disposable[] = [testCompletionsCmd, debugToggleCmd];

    await languageClient.start();
    logger.info("FlinkSQL Language Server started successfully");

    // Register listeners for diagnostic events and handle disposal
    languageClient.onDidChangeState((e) => {
      logger.info(`LanguageClient state changed from ${e.oldState} to ${e.newState}`);

      // Clean up when the client stops
      if (e.newState === State.Stopped) {
        logger.debug("Language client stopped, disposing commands");
        disposables.forEach((d) => d.dispose());
      }
    });

    // Check if the client is running
    if (languageClient.needsStart()) {
      logger.error("Language client still needs start after start() was called");
    } else if (languageClient.isRunning()) {
      logger.info("Language client is running");
    } else {
      logger.warn("Language client is not running even though start() completed");
    }

    languageClient.setTrace(Trace.Compact);

    // Return the language client
    return languageClient;
  } catch (e) {
    logger.error(`Error starting FlinkSQL language server: ${e}`);
    return null;
  }
}

/** Helper to convert vscode.Position to always have {line: 0...},
 * since CCloud Flink Language Server does not support multi-line completions at this time */
function convertToSingleLinePosition(
  document: vscode.TextDocument,
  position: vscode.Position,
): vscode.Position {
  const text = document.getText();
  const lines = text.split("\n");
  let singleLinePosition = 0;

  for (let i = 0; i < position.line; i++) {
    singleLinePosition += lines[i].length + 1; // +1 for the newline character
  }
  singleLinePosition += position.character;
  return new vscode.Position(0, singleLinePosition);
}
