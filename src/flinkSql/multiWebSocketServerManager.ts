import { TextDocument, workspace } from "vscode";
import { Message } from "vscode-languageclient/node";
import { CCLOUD_CONNECTION_ID } from "../constants";
import { uriMetadataSet } from "../emitters";
import { getEnvironments } from "../graphql/environments";
import { getCurrentOrganization } from "../graphql/organizations";
import { Logger } from "../logging";
import { CCloudFlinkComputePool } from "../models/flinkComputePool";
import { SIDECAR_PORT } from "../sidecar/constants";
import { UriMetadataKeys } from "../storage/constants";
import { ResourceManager } from "../storage/resourceManager";
import { ConnectionStatus, WebSocketServerManager } from "./websocketServerManager";

const logger = new Logger("flinkSql.multiWebSocketServerManager");

interface ServerInfo {
  url: string;
  manager: WebSocketServerManager;
  computePoolId: string | null; // null for default server
  isDefault: boolean;
}

/**
 * MultiWebSocketServerManager extends the basic WebSocketServerManager to handle
 * multiple websocket connections and route messages based on document properties
 */
export class MultiWebSocketServerManager {
  private servers: Map<string, ServerInfo> = new Map();
  private defaultServerId: string | null = null;
  private _messageHandler: ((message: Message) => void) | null = null;
  private openDocuments: Map<string, TextDocument> = new Map();
  private lastInitializeMessage: Message | null = null;

  constructor() {
    // Track open documents to know which servers we need
    workspace.onDidOpenTextDocument((doc) => {
      if (doc.languageId === "flinksql") {
        this.openDocuments.set(doc.uri.toString(), doc);
        this.maybeCreateServerForDocument(doc);
      }
    });

    workspace.onDidCloseTextDocument((doc) => {
      this.openDocuments.delete(doc.uri.toString());
      this.maybeCleanupUnusedServers();
    });

    // Listen for metadata changes to update server connections
    uriMetadataSet.event(async (uri) => {
      const doc = this.openDocuments.get(uri.toString());
      if (doc) {
        logger.debug(`Document metadata changed for ${uri.toString()}, updating server connection`);
        await this.handleDocumentMetadataChange(doc);
      }
    });
  }

  /**
   * Create a server for a document if needed based on its compute pool
   */
  private async maybeCreateServerForDocument(document: TextDocument): Promise<void> {
    const rm = ResourceManager.getInstance();
    const metadata = await rm.getUriMetadata(document.uri);
    const computePoolId = metadata?.[UriMetadataKeys.FLINK_COMPUTE_POOL_ID] ?? null;

    // If document has no compute pool, it will use the default server
    if (!computePoolId) {
      return;
    }

    // Check if we already have a server for this compute pool
    for (const [_, server] of this.servers.entries()) {
      if (server.computePoolId === computePoolId) {
        return; // Server already exists
      }
    }

    // Look up the URL for this compute pool
    try {
      const url = await this.buildWebSocketUrlForComputePool(computePoolId);
      if (url) {
        this.registerServer(computePoolId, url, false);
      }
    } catch (error) {
      logger.error(`Failed to create server for compute pool ${computePoolId}: ${error}`);
    }
  }

  /**
   * Clean up servers that are no longer needed
   */
  private async maybeCleanupUnusedServers(): Promise<void> {
    // Get all compute pool IDs from open documents
    const activeComputePoolIds = new Set<string>();
    for (const doc of this.openDocuments.values()) {
      const rm = ResourceManager.getInstance();
      const metadata = await rm.getUriMetadata(doc.uri);
      const computePoolId = metadata?.[UriMetadataKeys.FLINK_COMPUTE_POOL_ID];
      if (computePoolId) {
        activeComputePoolIds.add(computePoolId);
      }
    }

    // Remove servers that aren't the default and aren't used by any open documents
    for (const [id, server] of this.servers.entries()) {
      if (
        !server.isDefault &&
        server.computePoolId &&
        !activeComputePoolIds.has(server.computePoolId)
      ) {
        logger.debug(`Removing unused server for compute pool ${server.computePoolId}`);
        server.manager.dispose();
        this.servers.delete(id);
      }
    }
  }

  /**
   * Build WebSocket URL for a compute pool
   */
  private async buildWebSocketUrlForComputePool(computePoolId: string): Promise<string | null> {
    // This should use the same logic as FlinkLanguageClientManager.buildFlinkSqlWebSocketUrl
    // but we'll need to extract that into a shared utility
    try {
      const poolInfo = await this.lookupComputePoolInfo(computePoolId);
      if (!poolInfo) {
        return null;
      }
      const { organizationId, environmentId, region, provider } = poolInfo;
      return `ws://localhost:${SIDECAR_PORT}/flsp?connectionId=${CCLOUD_CONNECTION_ID}&region=${region}&provider=${provider}&environmentId=${environmentId}&organizationId=${organizationId}&computePoolId=${computePoolId}`;
    } catch (error) {
      logger.error(`Failed to build WebSocket URL for compute pool ${computePoolId}: ${error}`);
      return null;
    }
  }

  /**
   * Store initialization message and forward it to all servers
   */
  public handleInitializeMessage(message: Message): void {
    // Type guard to check if message is a request/notification with a method
    const isRequestOrNotification = (msg: Message): msg is Message & { method: string } => {
      return (
        typeof msg === "object" &&
        msg !== null &&
        "method" in msg &&
        typeof (msg as any).method === "string"
      );
    };

    if (!isRequestOrNotification(message) || message.method !== "initialize") {
      return;
    }

    logger.debug("Storing initialization message for new servers");
    this.lastInitializeMessage = message;

    // Forward to all existing servers
    for (const [id, server] of this.servers.entries()) {
      if (server.manager.connectionStatus === ConnectionStatus.CONNECTED) {
        logger.debug(`Forwarding initialization message to server for compute pool ${id}`);
        server.manager.queueMessage(message).catch((error) => {
          logger.error(`Failed to forward initialization message to server ${id}: ${error}`);
        });
      }
    }
  }

  /**
   * Register a new server with a compute pool ID and connection info
   */
  public registerServer(
    computePoolId: string | null,
    url: string,
    isDefault: boolean = false,
  ): void {
    const id = computePoolId ?? "default";

    if (this.servers.has(id)) {
      logger.warn(`Server for compute pool '${id}' already registered, replacing`);
      this.servers.get(id)?.manager.dispose();
    }

    const manager = new WebSocketServerManager(url);
    this.servers.set(id, { url, manager, computePoolId, isDefault });

    // Set message handler if we already have one
    if (this._messageHandler) {
      manager.setMessageHandler(this._messageHandler);
    }

    // Monitor connection status for logging and initialization
    manager.onConnectionStatusChange((status) => {
      logger.debug(`Server for compute pool '${id}' connection status: ${status}`);

      // When a server connects, send it the initialization message if we have one
      if (status === ConnectionStatus.CONNECTED && this.lastInitializeMessage) {
        logger.debug(`Sending stored initialization message to new server for compute pool ${id}`);
        manager.queueMessage(this.lastInitializeMessage).catch((error) => {
          logger.error(`Failed to send initialization message to new server ${id}: ${error}`);
        });
      }
    });

    if (isDefault) {
      this.defaultServerId = id;
    }
  }

  /**
   * Get the appropriate server for a given document
   */
  public async getServerForDocument(document: TextDocument): Promise<ServerInfo | null> {
    const rm = ResourceManager.getInstance();

    // Try to get metadata with a few retries
    let metadata = await rm.getUriMetadata(document.uri);
    logger.debug(`Metadata for ${document.uri.toString()}:`, metadata);
    let retries = 0;
    const maxRetries = 3;

    while (!metadata && retries < maxRetries) {
      logger.debug(
        `Metadata not found for ${document.uri.toString()}, retry ${retries + 1}/${maxRetries}`,
      );
      await new Promise((resolve) => setTimeout(resolve, 500));
      metadata = await rm.getUriMetadata(document.uri);
      retries++;
    }

    if (!metadata) {
      logger.debug(`No metadata found for ${document.uri.toString()} after ${maxRetries} retries`);
    } else {
      logger.debug(`Found metadata for ${document.uri.toString()}:`, metadata);
    }

    const computePoolId = metadata?.[UriMetadataKeys.FLINK_COMPUTE_POOL_ID] ?? null;

    // Try to find a server that matches the document's compute pool
    if (computePoolId) {
      const server = this.servers.get(computePoolId);
      if (server) {
        logger.debug(
          `Routing document ${document.uri.toString()} to server for compute pool '${computePoolId}'`,
        );
        return server;
      } else {
        logger.debug(`No server found for compute pool '${computePoolId}', will create one`);
        // Try to create a server for this compute pool
        await this.maybeCreateServerForDocument(document);
        // Check again after creation attempt
        const newServer = this.servers.get(computePoolId);
        if (newServer) {
          return newServer;
        }
      }
    }

    // Fall back to default server if available
    if (this.defaultServerId && this.servers.has(this.defaultServerId)) {
      logger.debug(`No specific server found for ${document.uri.toString()}, using default server`);
      return this.servers.get(this.defaultServerId) || null;
    }

    logger.warn(`No server found for document ${document.uri.toString()}`);
    return null;
  }

  /**
   * Set a message handler for all servers
   * @param handler The handler function to call with received messages
   */
  public setMessageHandler(handler: (message: Message) => void): void {
    this._messageHandler = handler;

    // Set handler for all existing servers
    for (const [_, serverInfo] of this.servers.entries()) {
      serverInfo.manager.setMessageHandler(handler);
    }
  }

  /**
   * Connect to a specific server by ID
   * @param id The server ID to connect to
   */
  public async connectToServer(id: string): Promise<void> {
    const server = this.servers.get(id);
    if (!server) {
      throw new Error(`Server with ID '${id}' not found`);
    }

    try {
      await server.manager.connect();
    } catch (error) {
      logger.error(`Failed to connect to server '${id}': ${error}`);
      throw error;
    }
  }

  /**
   * Connect to all registered servers
   */
  public async connectAll(): Promise<void> {
    const connectionPromises = Array.from(this.servers.entries()).map(async ([id, server]) => {
      try {
        await server.manager.connect();
        logger.debug(`Connected to server '${id}'`);
      } catch (error) {
        logger.error(`Failed to connect to server '${id}': ${error}`);
      }
    });

    await Promise.all(connectionPromises);
  }

  /**
   * Send a message to the default server
   * @param message The message to send
   */
  public async sendToDefaultServer(message: Message): Promise<void> {
    if (!this.defaultServerId || !this.servers.has(this.defaultServerId)) {
      throw new Error("No default server configured");
    }

    const serverInfo = this.servers.get(this.defaultServerId);
    if (!serverInfo) {
      throw new Error(`Default server '${this.defaultServerId}' not found`);
    }

    logger.debug(`Sending message to default server '${this.defaultServerId}'`);
    return serverInfo.manager.queueMessage(message);
  }

  /**
   * Send a message to the appropriate server based on document info
   * @param message The message to send
   * @param document The document context for routing
   */
  public async sendMessage(message: Message, document: TextDocument): Promise<void> {
    const server = await this.getServerForDocument(document);
    if (!server) {
      throw new Error(`No server available for document ${document.uri.toString()}`);
    }

    return server.manager.queueMessage(message);
  }

  /**
   * Get the connection status for a specific server
   * @param id The server ID
   */
  public getServerStatus(id: string): ConnectionStatus | null {
    const server = this.servers.get(id);
    if (!server) {
      return null;
    }
    return server.manager.connectionStatus;
  }

  /**
   * Check if any servers are currently connected
   */
  public hasConnectedServers(): boolean {
    for (const [_, server] of this.servers.entries()) {
      if (server.manager.connectionStatus === ConnectionStatus.CONNECTED) {
        return true;
      }
    }
    return false;
  }

  /**
   * Dispose all server connections
   */
  public dispose(): void {
    for (const [id, server] of this.servers.entries()) {
      logger.debug(`Disposing server '${id}'`);
      server.manager.dispose();
    }
    this.servers.clear();
    this.defaultServerId = null;
  }

  /**
   * Look up compute pool info from CCloud
   */
  private async lookupComputePoolInfo(computePoolId: string): Promise<{
    organizationId: string;
    environmentId: string;
    region: string;
    provider: string;
  } | null> {
    try {
      // Get the current org
      const currentOrg = await getCurrentOrganization();
      const organizationId = currentOrg?.id ?? "";
      if (!organizationId) {
        return null;
      }

      // Find the environment containing this compute pool
      const environments = await getEnvironments();
      if (!environments || environments.length === 0) {
        return null;
      }

      for (const env of environments) {
        const foundPool = env.flinkComputePools.find(
          (pool: CCloudFlinkComputePool) => pool.id === computePoolId,
        );
        if (foundPool) {
          return {
            organizationId,
            environmentId: env.id,
            region: foundPool.region,
            provider: foundPool.provider,
          };
        }
      }

      logger.warn(`Could not find environment containing compute pool ${computePoolId}`);
      return null;
    } catch (error) {
      logger.error("Error while looking up compute pool", error);
      return null;
    }
  }

  /**
   * Handle changes to a document's metadata, particularly compute pool changes
   */
  private async handleDocumentMetadataChange(document: TextDocument): Promise<void> {
    const rm = ResourceManager.getInstance();
    const metadata = await rm.getUriMetadata(document.uri);
    const newComputePoolId = metadata?.[UriMetadataKeys.FLINK_COMPUTE_POOL_ID] ?? null;

    // If the document already has a server, check if we need to change it
    for (const [id, server] of this.servers.entries()) {
      if (server.computePoolId === newComputePoolId) {
        // Document is already connected to the right server
        return;
      }
    }

    // If we get here, either:
    // 1. The document needs a new server for its compute pool
    // 2. The document should use the default server
    if (newComputePoolId) {
      // Create a new server for this compute pool if needed
      await this.maybeCreateServerForDocument(document);
    }

    // Clean up any unused servers
    await this.maybeCleanupUnusedServers();
  }
}
