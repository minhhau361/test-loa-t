package vn.hauminh.auth;

import io.papermc.paper.event.player.AsyncChatEvent;
import org.bukkit.Bukkit;
import org.bukkit.ChatColor;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.configuration.file.FileConfiguration;
import org.bukkit.configuration.file.YamlConfiguration;
import org.bukkit.entity.Entity;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.block.BlockBreakEvent;
import org.bukkit.event.block.BlockPlaceEvent;
import org.bukkit.event.entity.EntityDamageByEntityEvent;
import org.bukkit.event.entity.EntityDamageEvent;
import org.bukkit.event.entity.EntityPickupItemEvent;
import org.bukkit.event.inventory.InventoryClickEvent;
import org.bukkit.event.inventory.InventoryDragEvent;
import org.bukkit.event.player.PlayerCommandPreprocessEvent;
import org.bukkit.event.player.PlayerDropItemEvent;
import org.bukkit.event.player.PlayerInteractEntityEvent;
import org.bukkit.event.player.PlayerInteractEvent;
import org.bukkit.event.player.PlayerItemConsumeEvent;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.event.player.PlayerMoveEvent;
import org.bukkit.event.player.PlayerQuitEvent;
import org.bukkit.event.player.PlayerSwapHandItemsEvent;
import org.bukkit.plugin.java.JavaPlugin;
import org.mindrot.jbcrypt.BCrypt;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public final class AuthPlugin extends JavaPlugin implements Listener, CommandExecutor {
    private final Set<UUID> authenticated = ConcurrentHashMap.newKeySet();
    private final Set<UUID> registered = ConcurrentHashMap.newKeySet();
    private final Set<UUID> loaded = ConcurrentHashMap.newKeySet();
    private final Set<UUID> busy = ConcurrentHashMap.newKeySet();
    private String jdbcUrl;
    private Properties dbProperties;
    private FileConfiguration messages;

    @Override
    public void onEnable() {
        if (!getDataFolder().exists() && !getDataFolder().mkdirs()) {
            getLogger().severe("Could not create plugin data folder.");
            getServer().getPluginManager().disablePlugin(this);
            return;
        }
        saveDefaultConfig();
        saveResource("messages.yml", false);
        loadMessages();
        try {
            configureDatabase();
            createTable();
        } catch (Exception ex) {
            getLogger().severe("Could not initialize database: " + ex.getMessage());
            getServer().getPluginManager().disablePlugin(this);
            return;
        }
        getServer().getPluginManager().registerEvents(this, this);
        for (String commandName : new String[]{"register", "login", "logout"}) {
            if (getCommand(commandName) == null) {
                getLogger().severe("Command missing from plugin.yml: " + commandName);
                getServer().getPluginManager().disablePlugin(this);
                return;
            }
            getCommand(commandName).setExecutor(this);
        }
        getLogger().info("MiniAuth enabled.");
    }

    private void loadMessages() {
        messages = YamlConfiguration.loadConfiguration(new File(getDataFolder(), "messages.yml"));
    }

    private String raw(String key) { return messages.getString(key, "&cMissing message: " + key); }
    private String color(String text) { return ChatColor.translateAlternateColorCodes('&', text); }
    private void send(Player player, String key) {
        player.sendMessage(color(raw("prefix") + raw(key)));
    }
    private void send(Player player, String key, String token, String value) {
        player.sendMessage(color(raw("prefix") + raw(key).replace(token, value)));
    }
    private String kickMessage(String key) { return color(raw(key)); }

    private void configureDatabase() {
        String type = getConfig().getString("database.type", "sqlite").toLowerCase(Locale.ROOT);
        dbProperties = new Properties();
        if (type.equals("sqlite")) {
            File database = new File(getDataFolder(), getConfig().getString("database.sqlite.file", "auth.db"));
            jdbcUrl = "jdbc:sqlite:" + database.getAbsolutePath();
        } else if (type.equals("mysql")) {
            String host = getConfig().getString("database.mysql.host", "127.0.0.1");
            int port = getConfig().getInt("database.mysql.port", 3306);
            String schema = getConfig().getString("database.mysql.database", "miniauth");
            boolean ssl = getConfig().getBoolean("database.mysql.use-ssl", false);
            jdbcUrl = "jdbc:mysql://" + host + ":" + port + "/" + schema
                    + "?useSSL=" + ssl + "&allowPublicKeyRetrieval=true&serverTimezone=UTC";
            dbProperties.setProperty("user", getConfig().getString("database.mysql.username", "root"));
            dbProperties.setProperty("password", getConfig().getString("database.mysql.password", ""));
        } else {
            throw new IllegalArgumentException("database.type must be sqlite or mysql");
        }
    }

    private Connection connection() throws SQLException { return DriverManager.getConnection(jdbcUrl, dbProperties); }

    private void createTable() throws SQLException {
        try (Connection connection = connection(); var statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE IF NOT EXISTS accounts (uuid VARCHAR(36) PRIMARY KEY, password_hash VARCHAR(60) NOT NULL, created_at BIGINT NOT NULL)");
        }
    }

    private boolean accountExists(UUID uuid) throws SQLException {
        try (Connection connection = connection(); PreparedStatement statement = connection.prepareStatement("SELECT 1 FROM accounts WHERE uuid = ?")) {
            statement.setString(1, uuid.toString());
            try (ResultSet result = statement.executeQuery()) { return result.next(); }
        }
    }

    private String passwordHash(UUID uuid) throws SQLException {
        try (Connection connection = connection(); PreparedStatement statement = connection.prepareStatement("SELECT password_hash FROM accounts WHERE uuid = ?")) {
            statement.setString(1, uuid.toString());
            try (ResultSet result = statement.executeQuery()) { return result.next() ? result.getString(1) : null; }
        }
    }

    private boolean insertAccount(UUID uuid, String hash) throws SQLException {
        try (Connection connection = connection(); PreparedStatement statement = connection.prepareStatement("INSERT INTO accounts(uuid,password_hash,created_at) VALUES(?,?,?)")) {
            statement.setString(1, uuid.toString());
            statement.setString(2, hash);
            statement.setLong(3, System.currentTimeMillis());
            return statement.executeUpdate() == 1;
        } catch (SQLException ex) {
            if ("23000".equals(ex.getSQLState()) || "23505".equals(ex.getSQLState()) || ex.getErrorCode() == 19) return false;
            throw ex;
        }
    }

    private boolean isAuthenticated(Player player) { return authenticated.contains(player.getUniqueId()); }

    @EventHandler
    public void onJoin(PlayerJoinEvent event) {
        Player player = event.getPlayer();
        UUID uuid = player.getUniqueId();
        authenticated.remove(uuid); registered.remove(uuid); loaded.remove(uuid); busy.remove(uuid);
        send(player, "join-prompt");
        long timeoutTicks = Math.max(1, getConfig().getInt("auth-timeout-seconds", 60)) * 20L;
        Bukkit.getScheduler().runTaskLater(this, () -> {
            if (player.isOnline() && !isAuthenticated(player)) player.kickPlayer(kickMessage("timeout-kick"));
        }, timeoutTicks);
        Bukkit.getScheduler().runTaskAsynchronously(this, () -> {
            boolean exists;
            try { exists = accountExists(uuid); }
            catch (SQLException ex) {
                getLogger().warning("Account lookup failed: " + ex.getMessage());
                Bukkit.getScheduler().runTask(this, () -> {
                    if (player.isOnline()) player.kickPlayer(kickMessage("account-load-kick"));
                });
                return;
            }
            Bukkit.getScheduler().runTask(this, () -> {
                if (!player.isOnline()) return;
                loaded.add(uuid);
                if (exists) { registered.add(uuid); send(player, "account-exists"); }
                else send(player, "account-not-found");
            });
        });
    }

    @Override
    public boolean onCommand(CommandSender sender, Command command, String label, String[] args) {
        if (!(sender instanceof Player player)) { sender.sendMessage("This command can only be used in-game."); return true; }
        UUID uuid = player.getUniqueId();
        String name = command.getName().toLowerCase(Locale.ROOT);
        if (!loaded.contains(uuid)) { send(player, "loading"); return true; }
        if (name.equals("logout")) {
            if (authenticated.remove(uuid)) send(player, "logout-success");
            else send(player, "logout-not-logged-in");
            return true;
        }
        if (isAuthenticated(player)) { send(player, "already-authenticated"); return true; }
        if (busy.contains(uuid)) { send(player, "busy"); return true; }
        if (name.equals("register")) register(player, args);
        else if (name.equals("login")) login(player, args);
        return true;
    }

    private void register(Player player, String[] args) {
        UUID uuid = player.getUniqueId();
        if (args.length != 2) { send(player, "register-usage"); return; }
        if (registered.contains(uuid)) { send(player, "account-already-registered"); return; }
        if (!args[0].equals(args[1])) { send(player, "password-mismatch"); return; }
        int minLength = Math.max(1, getConfig().getInt("password-min-length", 8));
        if (args[0].length() < minLength) { send(player, "password-too-short", "%min%", String.valueOf(minLength)); return; }
        if (args[0].getBytes(StandardCharsets.UTF_8).length > 72) { send(player, "password-too-long"); return; }
        if (!busy.add(uuid)) { send(player, "busy"); return; }
        String password = args[0];
        send(player, "register-processing");
        Bukkit.getScheduler().runTaskAsynchronously(this, () -> {
            boolean created = false;
            Exception failure = null;
            try { created = insertAccount(uuid, BCrypt.hashpw(password, BCrypt.gensalt(12))); }
            catch (Exception ex) { failure = ex; }
            boolean succeeded = created;
            Exception error = failure;
            Bukkit.getScheduler().runTask(this, () -> {
                busy.remove(uuid);
                if (!player.isOnline()) return;
                if (error != null) { getLogger().warning("Registration failed: " + error.getMessage()); send(player, "register-failed"); }
                else if (!succeeded) { registered.add(uuid); send(player, "account-already-registered"); }
                else { registered.add(uuid); authenticated.add(uuid); send(player, "register-success"); }
            });
        });
    }

    private void login(Player player, String[] args) {
        UUID uuid = player.getUniqueId();
        if (args.length != 1) { send(player, "login-usage"); return; }
        if (!registered.contains(uuid)) { send(player, "not-registered"); return; }
        if (!busy.add(uuid)) { send(player, "busy"); return; }
        String password = args[0];
        send(player, "login-processing");
        Bukkit.getScheduler().runTaskAsynchronously(this, () -> {
            boolean valid = false;
            Exception failure = null;
            try { String hash = passwordHash(uuid); valid = hash != null && BCrypt.checkpw(password, hash); }
            catch (Exception ex) { failure = ex; }
            boolean succeeded = valid;
            Exception error = failure;
            Bukkit.getScheduler().runTask(this, () -> {
                busy.remove(uuid);
                if (!player.isOnline()) return;
                if (error != null) { getLogger().warning("Login failed: " + error.getMessage()); send(player, "login-failed"); }
                else if (!succeeded) send(player, "wrong-password");
                else { authenticated.add(uuid); send(player, "login-success"); }
            });
        });
    }

    @EventHandler public void onQuit(PlayerQuitEvent event) {
        UUID uuid = event.getPlayer().getUniqueId(); authenticated.remove(uuid); registered.remove(uuid); loaded.remove(uuid); busy.remove(uuid);
    }
    @EventHandler public void onMove(PlayerMoveEvent event) {
        if (isAuthenticated(event.getPlayer()) || event.getTo() == null) return;
        if (event.getFrom().getBlockX() != event.getTo().getBlockX() || event.getFrom().getBlockY() != event.getTo().getBlockY() || event.getFrom().getBlockZ() != event.getTo().getBlockZ()) event.setTo(event.getFrom());
    }
    @EventHandler public void onCommandPreprocess(PlayerCommandPreprocessEvent event) {
        Player player = event.getPlayer();
        if (isAuthenticated(player)) return;
        String message = event.getMessage().toLowerCase(Locale.ROOT);
        String root = message.substring(1).split("\\s+", 2)[0];
        if (root.contains(":")) root = root.substring(root.indexOf(':') + 1);
        if (!Set.of("login", "l", "register", "reg", "logout").contains(root)) { event.setCancelled(true); send(player, "auth-required"); }
    }
    @EventHandler public void onChat(AsyncChatEvent event) { if (!isAuthenticated(event.getPlayer())) event.setCancelled(true); }
    @EventHandler public void onInteract(PlayerInteractEvent event) { if (!isAuthenticated(event.getPlayer())) event.setCancelled(true); }
    @EventHandler public void onEntityInteract(PlayerInteractEntityEvent event) { if (!isAuthenticated(event.getPlayer())) event.setCancelled(true); }
    @EventHandler public void onBreak(BlockBreakEvent event) { if (!isAuthenticated(event.getPlayer())) event.setCancelled(true); }
    @EventHandler public void onPlace(BlockPlaceEvent event) { if (!isAuthenticated(event.getPlayer())) event.setCancelled(true); }
    @EventHandler public void onDrop(PlayerDropItemEvent event) { if (!isAuthenticated(event.getPlayer())) event.setCancelled(true); }
    @EventHandler public void onPickup(EntityPickupItemEvent event) { if (event.getEntity() instanceof Player player && !isAuthenticated(player)) event.setCancelled(true); }
    @EventHandler public void onInventoryClick(InventoryClickEvent event) { if (event.getWhoClicked() instanceof Player player && !isAuthenticated(player)) event.setCancelled(true); }
    @EventHandler public void onInventoryDrag(InventoryDragEvent event) { if (event.getWhoClicked() instanceof Player player && !isAuthenticated(player)) event.setCancelled(true); }
    @EventHandler public void onConsume(PlayerItemConsumeEvent event) { if (!isAuthenticated(event.getPlayer())) event.setCancelled(true); }
    @EventHandler public void onSwap(PlayerSwapHandItemsEvent event) { if (!isAuthenticated(event.getPlayer())) event.setCancelled(true); }
    @EventHandler public void onDamage(EntityDamageEvent event) { Entity entity = event.getEntity(); if (entity instanceof Player player && !isAuthenticated(player)) event.setCancelled(true); }
    @EventHandler public void onDamageByEntity(EntityDamageByEntityEvent event) { if (event.getDamager() instanceof Player player && !isAuthenticated(player)) event.setCancelled(true); }
}
