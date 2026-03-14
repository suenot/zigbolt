const std = @import("std");
const sbe = @import("sbe.zig");

// ============================================================================
// ZigBolt FIX Protocol Message Definitions (SBE-encoded)
// ============================================================================
//
// FIX (Financial Information eXchange) protocol messages encoded using
// Simple Binary Encoding (SBE) for ultra-low latency capital markets.
//
// Wire format per message:
//   [MessageHeader (8 bytes)] [block (BLOCK_LENGTH bytes)] [groups...]
//
// All fixed-block messages use extern struct layout for deterministic
// on-wire representation. Group-based messages use builder/reader patterns.
//

// ============================================================================
// Schema Constants
// ============================================================================

pub const SCHEMA_ID: u16 = 1;
pub const SCHEMA_VERSION: u16 = 1;

// ============================================================================
// FIX Enum Types
// ============================================================================

pub const Side = enum(u8) {
    buy = 1,
    sell = 2,

    pub const NULL: u8 = 255;
};

pub const OrdType = enum(u8) {
    market = 1,
    limit = 2,
    stop = 3,
    stop_limit = 4,
    market_limit = 75, // 'K'

    pub const NULL: u8 = 255;
};

pub const TimeInForce = enum(u8) {
    day = 0,
    gtc = 1, // Good Till Cancel
    ioc = 3, // Immediate or Cancel (Fill and Kill)
    gtd = 6, // Good Till Date

    pub const NULL: u8 = 255;
};

pub const ExecType = enum(u8) {
    new = 48, // '0'
    partial_fill = 49, // '1'
    fill = 50, // '2'
    canceled = 52, // '4'
    replaced = 53, // '5'
    rejected = 56, // '8'
    expired = 67, // 'C'
    trade = 70, // 'F'
    status = 73, // 'I'
};

pub const OrdStatus = enum(u8) {
    new = 48, // '0'
    partial_fill = 49, // '1'
    filled = 50, // '2'
    canceled = 52, // '4'
    replaced = 53, // '5'
    rejected = 56, // '8'
    expired = 67, // 'C'
};

pub const MDUpdateAction = enum(u8) {
    new = 0,
    change = 1,
    delete = 2,
    overlay = 5,
};

pub const MDEntryType = enum(u8) {
    bid = 48, // '0'
    offer = 49, // '1'
    trade = 50, // '2'
    opening_price = 52, // '4'
    settlement = 54, // '6'
    session_high = 55, // '7'
    session_low = 56, // '8'
    trade_volume = 66, // 'B'
    open_interest = 67, // 'C'
};

pub const HandInst = enum(u8) {
    automated = 49, // '1'
};

pub const CustomerOrFirm = enum(u8) {
    customer = 0,
    firm = 1,
};

pub const BooleanType = enum(u8) {
    false_val = 0,
    true_val = 1,
};

// ============================================================================
// Decimal Price Type
// ============================================================================

/// Fixed-point decimal for prices. Exponent is constant per schema (not on wire).
/// On wire: just the i64 mantissa.
/// Convention: exponent = -7 means price = mantissa * 10^-7
pub const Decimal64 = struct {
    mantissa: i64,

    pub const EXPONENT: i8 = -7; // CME convention
    pub const NULL_MANTISSA: i64 = std.math.maxInt(i64);
    pub const SCALE: f64 = 1e7; // 10^7

    pub fn fromFloat(price: f64) Decimal64 {
        return .{ .mantissa = @intFromFloat(@round(price * SCALE)) };
    }

    pub fn toFloat(self: Decimal64) f64 {
        const m: f64 = @floatFromInt(self.mantissa);
        return m / SCALE;
    }

    pub fn isNull(self: Decimal64) bool {
        return self.mantissa == NULL_MANTISSA;
    }

    pub fn null_val() Decimal64 {
        return .{ .mantissa = NULL_MANTISSA };
    }
};

pub const IntQty32 = struct {
    mantissa: i32,

    pub const EXPONENT: i8 = 0;
    pub const NULL: i32 = std.math.maxInt(i32);
};

// ============================================================================
// Fixed-Length String Type
// ============================================================================

pub fn FixedString(comptime N: usize) type {
    return extern struct {
        data: [N]u8,

        const Self = @This();

        /// Create from a slice, padding with zeros.
        pub fn fromSlice(s: []const u8) Self {
            var result: Self = .{ .data = [_]u8{0} ** N };
            const copy_len = @min(s.len, N);
            @memcpy(result.data[0..copy_len], s[0..copy_len]);
            return result;
        }

        /// Return a slice trimmed of trailing zero bytes.
        pub fn toSlice(self: *const Self) []const u8 {
            var len: usize = N;
            while (len > 0 and self.data[len - 1] == 0) {
                len -= 1;
            }
            return self.data[0..len];
        }

        /// Compare with a byte slice (trimmed comparison).
        pub fn eql(self: *const Self, other: []const u8) bool {
            const s = self.toSlice();
            return std.mem.eql(u8, s, other);
        }

        /// Return an empty (all-zero) instance.
        pub fn empty() Self {
            return .{ .data = [_]u8{0} ** N };
        }
    };
}

pub const String2 = FixedString(2);
pub const String3 = FixedString(3);
pub const String6 = FixedString(6); // Symbol
pub const String10 = FixedString(10);
pub const String12 = FixedString(12); // Account
pub const String20 = FixedString(20); // ClOrdID
pub const String23 = FixedString(23);

// ============================================================================
// Helper: SBE MessageHeader (8 bytes)
// ============================================================================

/// SBE MessageHeader: 8 bytes on wire.
/// Layout: blockLength(u16) | templateId(u16) | schemaId(u16) | version(u16)
pub const MessageHeader = extern struct {
    block_length: u16,
    template_id: u16,
    schema_id: u16,
    version: u16,

    pub const SIZE: usize = 8;

    pub fn init(comptime MsgType: type) MessageHeader {
        return .{
            .block_length = MsgType.BLOCK_LENGTH,
            .template_id = MsgType.TEMPLATE_ID,
            .schema_id = SCHEMA_ID,
            .version = SCHEMA_VERSION,
        };
    }
};

/// SBE Group header: numInGroup(u16) | blockLength(u16)
pub const GroupHeader = extern struct {
    block_length: u16,
    num_in_group: u16,

    pub const SIZE: usize = 4;
};

// ============================================================================
// Encode/Decode Helpers
// ============================================================================

/// Encode a fixed-block message (no groups) into buf.
/// Returns the total number of bytes written (header + block).
fn encodeFixedMessage(comptime MsgType: type, self: *const MsgType, buf: []u8) usize {
    const total = MessageHeader.SIZE + MsgType.BLOCK_LENGTH;
    if (buf.len < total) return 0;

    // Write header
    const hdr = MessageHeader.init(MsgType);
    const hdr_bytes = std.mem.asBytes(&hdr);
    @memcpy(buf[0..MessageHeader.SIZE], hdr_bytes);

    // Write block
    const msg_bytes: *const [MsgType.BLOCK_LENGTH]u8 = @ptrCast(self);
    @memcpy(buf[MessageHeader.SIZE..total], msg_bytes);

    return total;
}

/// Return type for decodeFixedMessage
fn DecodeResult(comptime MsgType: type) type {
    return struct { header: MessageHeader, msg: *align(1) const MsgType };
}

/// Decode a fixed-block message (no groups) from buf.
/// Returns the header and a zero-copy pointer into the buffer.
fn decodeFixedMessage(comptime MsgType: type, buf: []const u8) DecodeResult(MsgType) {
    const total = MessageHeader.SIZE + MsgType.BLOCK_LENGTH;
    if (buf.len < total) unreachable;

    const hdr: *align(1) const MessageHeader = @ptrCast(buf[0..MessageHeader.SIZE]);
    const msg: *align(1) const MsgType = @ptrCast(buf[MessageHeader.SIZE..total]);

    return .{ .header = hdr.*, .msg = msg };
}

// ============================================================================
// Message Definitions
// ============================================================================

// --------------------------------------------------------------------------
// NewOrderSingle (templateId = 68, FIX MsgType 'D')
// --------------------------------------------------------------------------

pub const NewOrderSingle = extern struct {
    account: String12,
    cl_ord_id: String20,
    hand_inst: HandInst,
    cust_order_handling_inst: u8,
    order_qty: i32,
    ord_type: OrdType,
    price_mantissa: i64,
    side: Side,
    symbol: String6,
    time_in_force: TimeInForce,
    transact_time: u64,
    manual_order_indicator: BooleanType,
    alloc_account: String10,
    stop_px_mantissa: i64,
    security_desc: String20,
    min_qty: i32,
    security_type: String3,
    customer_or_firm: CustomerOrFirm,
    max_show: i32,
    expire_date: u16,
    self_match_prevention_id: String12,
    cti_code: u8,
    give_up_firm: String3,
    cmta_giveup_cd: String2,
    correlation_cl_ord_id: String20,

    pub const TEMPLATE_ID: u16 = 68;
    pub const BLOCK_LENGTH: u16 = @sizeOf(NewOrderSingle);

    pub fn encode(self: *const NewOrderSingle, buf: []u8) usize {
        return encodeFixedMessage(NewOrderSingle, self, buf);
    }

    pub fn decode(buf: []const u8) DecodeResult(NewOrderSingle) {
        return decodeFixedMessage(NewOrderSingle, buf);
    }
};

// --------------------------------------------------------------------------
// OrderCancelRequest (templateId = 70, FIX MsgType 'F')
// --------------------------------------------------------------------------

pub const OrderCancelRequest = extern struct {
    account: String12,
    cl_ord_id: String20,
    order_id: i64,
    orig_cl_ord_id: String20,
    side: Side,
    symbol: String6,
    transact_time: u64,
    manual_order_indicator: BooleanType,
    security_desc: String20,
    security_type: String3,
    correlation_cl_ord_id: String20,

    pub const TEMPLATE_ID: u16 = 70;
    pub const BLOCK_LENGTH: u16 = @sizeOf(OrderCancelRequest);

    pub fn encode(self: *const OrderCancelRequest, buf: []u8) usize {
        return encodeFixedMessage(OrderCancelRequest, self, buf);
    }

    pub fn decode(buf: []const u8) DecodeResult(OrderCancelRequest) {
        return decodeFixedMessage(OrderCancelRequest, buf);
    }
};

// --------------------------------------------------------------------------
// OrderCancelReplaceRequest (templateId = 71, FIX MsgType 'G')
// --------------------------------------------------------------------------

pub const OrderCancelReplaceRequest = extern struct {
    account: String12,
    cl_ord_id: String20,
    order_id: i64,
    hand_inst: HandInst,
    cust_order_handling_inst: u8,
    order_qty: i32,
    ord_type: OrdType,
    orig_cl_ord_id: String20,
    price_mantissa: i64,
    side: Side,
    symbol: String6,
    time_in_force: TimeInForce,
    transact_time: u64,
    manual_order_indicator: BooleanType,
    alloc_account: String10,
    stop_px_mantissa: i64,
    security_desc: String20,
    min_qty: i32,
    security_type: String3,
    customer_or_firm: CustomerOrFirm,
    max_show: i32,
    expire_date: u16,
    self_match_prevention_id: String12,
    cti_code: u8,
    give_up_firm: String3,
    cmta_giveup_cd: String2,
    correlation_cl_ord_id: String20,

    pub const TEMPLATE_ID: u16 = 71;
    pub const BLOCK_LENGTH: u16 = @sizeOf(OrderCancelReplaceRequest);

    pub fn encode(self: *const OrderCancelReplaceRequest, buf: []u8) usize {
        return encodeFixedMessage(OrderCancelReplaceRequest, self, buf);
    }

    pub fn decode(buf: []const u8) DecodeResult(OrderCancelReplaceRequest) {
        return decodeFixedMessage(OrderCancelReplaceRequest, buf);
    }
};

// --------------------------------------------------------------------------
// ExecutionReport (templateId = 56, FIX MsgType '8')
// --------------------------------------------------------------------------

pub const ExecutionReport = extern struct {
    order_id: i64,
    cl_ord_id: String20,
    orig_cl_ord_id: String20,
    exec_type: ExecType,
    ord_status: OrdStatus,
    symbol: String6,
    side: Side,
    leaves_qty: i32,
    cum_qty: i32,
    avg_px_mantissa: i64,
    transact_time: u64,
    price_mantissa: i64,
    order_qty: i32,
    security_desc: String20,
    security_type: String3,
    last_qty: i32,
    last_px_mantissa: i64,

    pub const TEMPLATE_ID: u16 = 56;
    pub const BLOCK_LENGTH: u16 = @sizeOf(ExecutionReport);

    pub fn encode(self: *const ExecutionReport, buf: []u8) usize {
        return encodeFixedMessage(ExecutionReport, self, buf);
    }

    pub fn decode(buf: []const u8) DecodeResult(ExecutionReport) {
        return decodeFixedMessage(ExecutionReport, buf);
    }
};

// --------------------------------------------------------------------------
// Heartbeat (templateId = 48, FIX MsgType '0')
// --------------------------------------------------------------------------

pub const Heartbeat = extern struct {
    timestamp: u64,

    pub const TEMPLATE_ID: u16 = 48;
    pub const BLOCK_LENGTH: u16 = @sizeOf(Heartbeat);

    pub fn encode(self: *const Heartbeat, buf: []u8) usize {
        return encodeFixedMessage(Heartbeat, self, buf);
    }

    pub fn decode(buf: []const u8) DecodeResult(Heartbeat) {
        return decodeFixedMessage(Heartbeat, buf);
    }
};

// --------------------------------------------------------------------------
// Logon (templateId = 65, FIX MsgType 'A')
// --------------------------------------------------------------------------

pub const Logon = extern struct {
    heart_bt_int: u32,
    username: String20,
    password: String20,
    reset_seq_num: BooleanType,

    pub const TEMPLATE_ID: u16 = 65;
    pub const BLOCK_LENGTH: u16 = @sizeOf(Logon);

    pub fn encode(self: *const Logon, buf: []u8) usize {
        return encodeFixedMessage(Logon, self, buf);
    }

    pub fn decode(buf: []const u8) DecodeResult(Logon) {
        return decodeFixedMessage(Logon, buf);
    }
};

// --------------------------------------------------------------------------
// Logout (templateId = 53, FIX MsgType '5')
// --------------------------------------------------------------------------

pub const Logout = extern struct {
    session_status: u8,
    text: String20,

    pub const TEMPLATE_ID: u16 = 53;
    pub const BLOCK_LENGTH: u16 = @sizeOf(Logout);

    pub fn encode(self: *const Logout, buf: []u8) usize {
        return encodeFixedMessage(Logout, self, buf);
    }

    pub fn decode(buf: []const u8) DecodeResult(Logout) {
        return decodeFixedMessage(Logout, buf);
    }
};

// --------------------------------------------------------------------------
// MarketDataIncrementalRefresh (templateId = 88, FIX MsgType 'X')
// --------------------------------------------------------------------------
// This message uses a repeating group, so it cannot be a simple extern struct.
// We define the root block and group entry as extern structs, and provide
// builder/reader patterns for encoding/decoding.

pub const MdIncRefreshRoot = extern struct {
    trade_date: u16,

    pub const TEMPLATE_ID: u16 = 88;
    pub const BLOCK_LENGTH: u16 = @sizeOf(MdIncRefreshRoot);
};

pub const MdIncGrpEntry = extern struct {
    md_update_action: MDUpdateAction,
    md_price_level: u8,
    md_entry_type: MDEntryType,
    security_id: u64,
    rpt_seq: u32,
    md_entry_px_mantissa: i64,
    number_of_orders: u32,
    md_entry_time: u64,
    md_entry_size: i32,
    aggressor_side: Side,
    trade_id: u64,

    pub const BLOCK_LENGTH: u16 = @sizeOf(MdIncGrpEntry);
};

pub const MarketDataIncrementalRefresh = struct {
    pub const TEMPLATE_ID: u16 = 88;

    /// Encode an incremental refresh message with a slice of group entries.
    /// Returns the total number of bytes written.
    pub fn encode(
        trade_date: u16,
        entries: []const MdIncGrpEntry,
        buf: []u8,
    ) usize {
        const root_len = MdIncRefreshRoot.BLOCK_LENGTH;
        const entry_len = MdIncGrpEntry.BLOCK_LENGTH;
        const total = MessageHeader.SIZE + root_len + GroupHeader.SIZE +
            @as(usize, entry_len) * entries.len;
        if (buf.len < total) return 0;

        var offset: usize = 0;

        // Message header
        const hdr = MessageHeader{
            .block_length = root_len,
            .template_id = TEMPLATE_ID,
            .schema_id = SCHEMA_ID,
            .version = SCHEMA_VERSION,
        };
        @memcpy(buf[offset..][0..MessageHeader.SIZE], std.mem.asBytes(&hdr));
        offset += MessageHeader.SIZE;

        // Root block
        const root = MdIncRefreshRoot{ .trade_date = trade_date };
        @memcpy(buf[offset..][0..root_len], std.mem.asBytes(&root)[0..root_len]);
        offset += root_len;

        // Group header
        const grp_hdr = GroupHeader{
            .block_length = entry_len,
            .num_in_group = @intCast(entries.len),
        };
        @memcpy(buf[offset..][0..GroupHeader.SIZE], std.mem.asBytes(&grp_hdr));
        offset += GroupHeader.SIZE;

        // Group entries
        for (entries) |*entry| {
            const entry_bytes: *const [entry_len]u8 = @ptrCast(entry);
            @memcpy(buf[offset..][0..entry_len], entry_bytes);
            offset += entry_len;
        }

        return offset;
    }

    /// Decoded result for incremental refresh.
    pub const Decoded = struct {
        header: MessageHeader,
        trade_date: u16,
        num_entries: u16,
        entries_ptr: [*]align(1) const MdIncGrpEntry,

        pub fn entry(self: Decoded, idx: usize) *align(1) const MdIncGrpEntry {
            if (idx >= self.num_entries) unreachable;
            return &self.entries_ptr[idx];
        }
    };

    /// Decode an incremental refresh message from buf.
    pub fn decode(buf: []const u8) Decoded {
        const min_size = MessageHeader.SIZE + MdIncRefreshRoot.BLOCK_LENGTH + GroupHeader.SIZE;
        if (buf.len < min_size) unreachable;

        var offset: usize = 0;

        // Header
        const hdr: *align(1) const MessageHeader = @ptrCast(buf[offset..][0..MessageHeader.SIZE]);
        offset += MessageHeader.SIZE;

        // Root block
        const root: *align(1) const MdIncRefreshRoot = @ptrCast(buf[offset..][0..MdIncRefreshRoot.BLOCK_LENGTH]);
        offset += hdr.block_length; // use header's block_length for forward compat

        // Group header
        const grp_hdr: *align(1) const GroupHeader = @ptrCast(buf[offset..][0..GroupHeader.SIZE]);
        offset += GroupHeader.SIZE;

        // Entries pointer
        const entries_ptr: [*]align(1) const MdIncGrpEntry = @ptrCast(buf[offset..].ptr);

        return .{
            .header = hdr.*,
            .trade_date = root.trade_date,
            .num_entries = grp_hdr.num_in_group,
            .entries_ptr = entries_ptr,
        };
    }
};

// --------------------------------------------------------------------------
// MarketDataSnapshotFullRefresh (templateId = 87, FIX MsgType 'W')
// --------------------------------------------------------------------------

pub const MdSnapRefreshRoot = extern struct {
    security_id: u64,
    symbol: String6,
    trading_session_id: u8,

    pub const TEMPLATE_ID: u16 = 87;
    pub const BLOCK_LENGTH: u16 = @sizeOf(MdSnapRefreshRoot);
};

pub const MdFullGrpEntry = extern struct {
    md_entry_type: MDEntryType,
    md_entry_px_mantissa: i64,
    md_entry_size: i32,
    number_of_orders: u32,

    pub const BLOCK_LENGTH: u16 = @sizeOf(MdFullGrpEntry);
};

pub const MarketDataSnapshotFullRefresh = struct {
    pub const TEMPLATE_ID: u16 = 87;

    /// Encode a full refresh snapshot message.
    pub fn encode(
        security_id: u64,
        symbol: String6,
        trading_session_id: u8,
        entries: []const MdFullGrpEntry,
        buf: []u8,
    ) usize {
        const root_len = MdSnapRefreshRoot.BLOCK_LENGTH;
        const entry_len = MdFullGrpEntry.BLOCK_LENGTH;
        const total = MessageHeader.SIZE + root_len + GroupHeader.SIZE +
            @as(usize, entry_len) * entries.len;
        if (buf.len < total) return 0;

        var offset: usize = 0;

        // Message header
        const hdr = MessageHeader{
            .block_length = root_len,
            .template_id = TEMPLATE_ID,
            .schema_id = SCHEMA_ID,
            .version = SCHEMA_VERSION,
        };
        @memcpy(buf[offset..][0..MessageHeader.SIZE], std.mem.asBytes(&hdr));
        offset += MessageHeader.SIZE;

        // Root block
        const root = MdSnapRefreshRoot{
            .security_id = security_id,
            .symbol = symbol,
            .trading_session_id = trading_session_id,
        };
        @memcpy(buf[offset..][0..root_len], std.mem.asBytes(&root)[0..root_len]);
        offset += root_len;

        // Group header
        const grp_hdr = GroupHeader{
            .block_length = entry_len,
            .num_in_group = @intCast(entries.len),
        };
        @memcpy(buf[offset..][0..GroupHeader.SIZE], std.mem.asBytes(&grp_hdr));
        offset += GroupHeader.SIZE;

        // Entries
        for (entries) |*entry| {
            const entry_bytes: *const [entry_len]u8 = @ptrCast(entry);
            @memcpy(buf[offset..][0..entry_len], entry_bytes);
            offset += entry_len;
        }

        return offset;
    }

    pub const Decoded = struct {
        header: MessageHeader,
        security_id: u64,
        symbol: String6,
        trading_session_id: u8,
        num_entries: u16,
        entries_ptr: [*]align(1) const MdFullGrpEntry,

        pub fn entry(self: Decoded, idx: usize) *align(1) const MdFullGrpEntry {
            if (idx >= self.num_entries) unreachable;
            return &self.entries_ptr[idx];
        }
    };

    pub fn decode(buf: []const u8) Decoded {
        const min_size = MessageHeader.SIZE + MdSnapRefreshRoot.BLOCK_LENGTH + GroupHeader.SIZE;
        if (buf.len < min_size) unreachable;

        var offset: usize = 0;

        const hdr: *align(1) const MessageHeader = @ptrCast(buf[offset..][0..MessageHeader.SIZE]);
        offset += MessageHeader.SIZE;

        const root: *align(1) const MdSnapRefreshRoot = @ptrCast(buf[offset..][0..MdSnapRefreshRoot.BLOCK_LENGTH]);
        offset += hdr.block_length;

        const grp_hdr: *align(1) const GroupHeader = @ptrCast(buf[offset..][0..GroupHeader.SIZE]);
        offset += GroupHeader.SIZE;

        const entries_ptr: [*]align(1) const MdFullGrpEntry = @ptrCast(buf[offset..].ptr);

        return .{
            .header = hdr.*,
            .security_id = root.security_id,
            .symbol = root.symbol,
            .trading_session_id = root.trading_session_id,
            .num_entries = grp_hdr.num_in_group,
            .entries_ptr = entries_ptr,
        };
    }
};

// --------------------------------------------------------------------------
// MassQuote (templateId = 105, FIX MsgType 'i')
// --------------------------------------------------------------------------
// MassQuote has nested groups: QuoteSets > QuoteEntries.
// We define flat structs and a builder/reader for the nested structure.

pub const MassQuoteRoot = extern struct {
    quote_req_id: String23,
    quote_id: String10,
    mm_account: String12,

    pub const TEMPLATE_ID: u16 = 105;
    pub const BLOCK_LENGTH: u16 = @sizeOf(MassQuoteRoot);
};

pub const QuoteSetEntry = extern struct {
    quote_set_id: String3,
    underlying_security_desc: String20,

    pub const BLOCK_LENGTH: u16 = @sizeOf(QuoteSetEntry);
};

pub const QuoteEntry = extern struct {
    quote_entry_id: String10,
    symbol: String6,
    security_desc: String20,
    bid_px_mantissa: i64,
    bid_size: i64,
    offer_px_mantissa: i64,
    offer_size: i64,
    transact_time: u64,

    pub const BLOCK_LENGTH: u16 = @sizeOf(QuoteEntry);
};

/// A single quote set with its nested quote entries, used for building MassQuote.
pub const QuoteSetWithEntries = struct {
    set: QuoteSetEntry,
    entries: []const QuoteEntry,
};

pub const MassQuote = struct {
    pub const TEMPLATE_ID: u16 = 105;

    /// Encode a MassQuote with nested groups.
    /// Layout: Header | Root | QuoteSets GroupHeader | for each set: { SetBlock | QuoteEntries GroupHeader | entries... }
    pub fn encode(
        root: MassQuoteRoot,
        quote_sets: []const QuoteSetWithEntries,
        buf: []u8,
    ) usize {
        // Calculate total size
        var total: usize = MessageHeader.SIZE + MassQuoteRoot.BLOCK_LENGTH + GroupHeader.SIZE;
        for (quote_sets) |qs| {
            total += QuoteSetEntry.BLOCK_LENGTH + GroupHeader.SIZE +
                @as(usize, QuoteEntry.BLOCK_LENGTH) * qs.entries.len;
        }
        if (buf.len < total) return 0;

        var offset: usize = 0;

        // Message header
        const hdr = MessageHeader{
            .block_length = MassQuoteRoot.BLOCK_LENGTH,
            .template_id = TEMPLATE_ID,
            .schema_id = SCHEMA_ID,
            .version = SCHEMA_VERSION,
        };
        @memcpy(buf[offset..][0..MessageHeader.SIZE], std.mem.asBytes(&hdr));
        offset += MessageHeader.SIZE;

        // Root
        const root_len = MassQuoteRoot.BLOCK_LENGTH;
        @memcpy(buf[offset..][0..root_len], std.mem.asBytes(&root)[0..root_len]);
        offset += root_len;

        // QuoteSets group header
        const sets_hdr = GroupHeader{
            .block_length = QuoteSetEntry.BLOCK_LENGTH,
            .num_in_group = @intCast(quote_sets.len),
        };
        @memcpy(buf[offset..][0..GroupHeader.SIZE], std.mem.asBytes(&sets_hdr));
        offset += GroupHeader.SIZE;

        // Each quote set
        for (quote_sets) |qs| {
            // Set block
            const set_len = QuoteSetEntry.BLOCK_LENGTH;
            const set_bytes: *const [set_len]u8 = @ptrCast(&qs.set);
            @memcpy(buf[offset..][0..set_len], set_bytes);
            offset += set_len;

            // QuoteEntries group header
            const entries_hdr = GroupHeader{
                .block_length = QuoteEntry.BLOCK_LENGTH,
                .num_in_group = @intCast(qs.entries.len),
            };
            @memcpy(buf[offset..][0..GroupHeader.SIZE], std.mem.asBytes(&entries_hdr));
            offset += GroupHeader.SIZE;

            // Entries
            for (qs.entries) |*entry| {
                const entry_len = QuoteEntry.BLOCK_LENGTH;
                const entry_bytes: *const [entry_len]u8 = @ptrCast(entry);
                @memcpy(buf[offset..][0..entry_len], entry_bytes);
                offset += entry_len;
            }
        }

        return offset;
    }

    /// Decoded MassQuote result. Provides methods to iterate nested groups.
    pub const Decoded = struct {
        header: MessageHeader,
        root: *align(1) const MassQuoteRoot,
        num_quote_sets: u16,
        /// Raw buffer starting at first quote set block.
        group_data: []const u8,

        pub const QuoteSetDecoded = struct {
            set: *align(1) const QuoteSetEntry,
            num_entries: u16,
            entries_ptr: [*]align(1) const QuoteEntry,

            pub fn entry(self: QuoteSetDecoded, idx: usize) *align(1) const QuoteEntry {
                if (idx >= self.num_entries) unreachable;
                return &self.entries_ptr[idx];
            }
        };

        /// Iterate over quote sets by index. Must be called in order (0, 1, 2, ...).
        /// For simplicity, re-walks from the start each time.
        pub fn quoteSet(self: Decoded, set_idx: usize) QuoteSetDecoded {
            if (set_idx >= self.num_quote_sets) unreachable;

            var offset: usize = 0;
            var i: usize = 0;
            while (i < set_idx) : (i += 1) {
                // Skip set block
                offset += QuoteSetEntry.BLOCK_LENGTH;
                // Read entries group header
                const ehdr: *align(1) const GroupHeader = @ptrCast(self.group_data[offset..][0..GroupHeader.SIZE]);
                offset += GroupHeader.SIZE;
                // Skip entries
                offset += @as(usize, ehdr.num_in_group) * ehdr.block_length;
            }

            const set: *align(1) const QuoteSetEntry = @ptrCast(self.group_data[offset..][0..QuoteSetEntry.BLOCK_LENGTH]);
            offset += QuoteSetEntry.BLOCK_LENGTH;

            const ehdr: *align(1) const GroupHeader = @ptrCast(self.group_data[offset..][0..GroupHeader.SIZE]);
            offset += GroupHeader.SIZE;

            const entries_ptr: [*]align(1) const QuoteEntry = @ptrCast(self.group_data[offset..].ptr);

            return .{
                .set = set,
                .num_entries = ehdr.num_in_group,
                .entries_ptr = entries_ptr,
            };
        }
    };

    pub fn decode(buf: []const u8) Decoded {
        const min_size = MessageHeader.SIZE + MassQuoteRoot.BLOCK_LENGTH + GroupHeader.SIZE;
        if (buf.len < min_size) unreachable;

        var offset: usize = 0;

        const hdr: *align(1) const MessageHeader = @ptrCast(buf[offset..][0..MessageHeader.SIZE]);
        offset += MessageHeader.SIZE;

        const root: *align(1) const MassQuoteRoot = @ptrCast(buf[offset..][0..MassQuoteRoot.BLOCK_LENGTH]);
        offset += hdr.block_length;

        const sets_hdr: *align(1) const GroupHeader = @ptrCast(buf[offset..][0..GroupHeader.SIZE]);
        offset += GroupHeader.SIZE;

        return .{
            .header = hdr.*,
            .root = root,
            .num_quote_sets = sets_hdr.num_in_group,
            .group_data = buf[offset..],
        };
    }
};

// ============================================================================
// Tests
// ============================================================================

const testing = std.testing;

test "Decimal64 — price conversion accuracy" {
    const prices = [_]f64{ 0.0, 1.0, 100.5, 12345.6789012, -99.99, 0.0000001 };
    for (prices) |p| {
        const d = Decimal64.fromFloat(p);
        const back = d.toFloat();
        try testing.expectApproxEqAbs(p, back, 1e-7);
    }
}

test "Decimal64 — null value" {
    const d = Decimal64.null_val();
    try testing.expect(d.isNull());

    const d2 = Decimal64.fromFloat(42.0);
    try testing.expect(!d2.isNull());
}

test "FixedString — padding and trimming" {
    const s = String6.fromSlice("ABC");
    try testing.expectEqualSlices(u8, "ABC", s.toSlice());
    try testing.expect(s.eql("ABC"));
    try testing.expect(!s.eql("ABCD"));

    // Full length
    const s2 = String6.fromSlice("ABCDEF");
    try testing.expectEqualSlices(u8, "ABCDEF", s2.toSlice());

    // Truncation: input longer than N
    const s3 = String6.fromSlice("ABCDEFGH");
    try testing.expectEqual(@as(usize, 6), s3.toSlice().len);

    // Empty
    const s4 = String6.fromSlice("");
    try testing.expectEqual(@as(usize, 0), s4.toSlice().len);
}

test "Enum roundtrip — all Side values" {
    const sides = [_]Side{ .buy, .sell };
    for (sides) |s| {
        const raw: u8 = @intFromEnum(s);
        const back: Side = @enumFromInt(raw);
        try testing.expectEqual(s, back);
    }
}

test "Enum roundtrip — all OrdType values" {
    const types = [_]OrdType{ .market, .limit, .stop, .stop_limit, .market_limit };
    for (types) |t| {
        const raw: u8 = @intFromEnum(t);
        const back: OrdType = @enumFromInt(raw);
        try testing.expectEqual(t, back);
    }
}

test "Enum roundtrip — all ExecType values" {
    const types = [_]ExecType{
        .new, .partial_fill, .fill, .canceled, .replaced, .rejected, .expired, .trade, .status,
    };
    for (types) |t| {
        const raw: u8 = @intFromEnum(t);
        const back: ExecType = @enumFromInt(raw);
        try testing.expectEqual(t, back);
    }
}

test "Enum roundtrip — all OrdStatus values" {
    const statuses = [_]OrdStatus{
        .new, .partial_fill, .filled, .canceled, .replaced, .rejected, .expired,
    };
    for (statuses) |s| {
        const raw: u8 = @intFromEnum(s);
        const back: OrdStatus = @enumFromInt(raw);
        try testing.expectEqual(s, back);
    }
}

test "Enum roundtrip — all MDUpdateAction values" {
    const actions = [_]MDUpdateAction{ .new, .change, .delete, .overlay };
    for (actions) |a| {
        const raw: u8 = @intFromEnum(a);
        const back: MDUpdateAction = @enumFromInt(raw);
        try testing.expectEqual(a, back);
    }
}

test "Enum roundtrip — all MDEntryType values" {
    const types = [_]MDEntryType{
        .bid, .offer, .trade, .opening_price, .settlement, .session_high, .session_low, .trade_volume, .open_interest,
    };
    for (types) |t| {
        const raw: u8 = @intFromEnum(t);
        const back: MDEntryType = @enumFromInt(raw);
        try testing.expectEqual(t, back);
    }
}

test "NewOrderSingle — encode/decode roundtrip" {
    const msg = NewOrderSingle{
        .account = String12.fromSlice("ACCT001"),
        .cl_ord_id = String20.fromSlice("ORD-20240101-001"),
        .hand_inst = .automated,
        .cust_order_handling_inst = 0,
        .order_qty = 100,
        .ord_type = .limit,
        .price_mantissa = Decimal64.fromFloat(150.25).mantissa,
        .side = .buy,
        .symbol = String6.fromSlice("AAPL"),
        .time_in_force = .day,
        .transact_time = 1_700_000_000_000_000_000,
        .manual_order_indicator = .false_val,
        .alloc_account = String10.fromSlice("ALLOC1"),
        .stop_px_mantissa = Decimal64.null_val().mantissa,
        .security_desc = String20.fromSlice("Apple Inc"),
        .min_qty = IntQty32.NULL,
        .security_type = String3.fromSlice("CS"),
        .customer_or_firm = .customer,
        .max_show = IntQty32.NULL,
        .expire_date = 0,
        .self_match_prevention_id = String12.empty(),
        .cti_code = 0,
        .give_up_firm = String3.empty(),
        .cmta_giveup_cd = String2.empty(),
        .correlation_cl_ord_id = String20.empty(),
    };

    var buf: [1024]u8 = undefined;
    const written = msg.encode(&buf);
    try testing.expect(written > 0);
    try testing.expectEqual(MessageHeader.SIZE + NewOrderSingle.BLOCK_LENGTH, written);

    const decoded = NewOrderSingle.decode(buf[0..written]);
    try testing.expectEqual(@as(u16, 68), decoded.header.template_id);
    try testing.expectEqual(SCHEMA_ID, decoded.header.schema_id);
    try testing.expect(decoded.msg.account.eql("ACCT001"));
    try testing.expect(decoded.msg.cl_ord_id.eql("ORD-20240101-001"));
    try testing.expectEqual(Side.buy, decoded.msg.side);
    try testing.expectEqual(OrdType.limit, decoded.msg.ord_type);
    try testing.expectEqual(@as(i32, 100), decoded.msg.order_qty);
    try testing.expectEqual(msg.price_mantissa, decoded.msg.price_mantissa);
    try testing.expectEqual(msg.transact_time, decoded.msg.transact_time);
    try testing.expect(decoded.msg.symbol.eql("AAPL"));
}

test "ExecutionReport — encode/decode roundtrip" {
    const msg = ExecutionReport{
        .order_id = 12345678,
        .cl_ord_id = String20.fromSlice("ORD-001"),
        .orig_cl_ord_id = String20.empty(),
        .exec_type = .fill,
        .ord_status = .filled,
        .symbol = String6.fromSlice("MSFT"),
        .side = .sell,
        .leaves_qty = 0,
        .cum_qty = 500,
        .avg_px_mantissa = Decimal64.fromFloat(420.50).mantissa,
        .transact_time = 1_700_000_000_000_000_000,
        .price_mantissa = Decimal64.fromFloat(420.50).mantissa,
        .order_qty = 500,
        .security_desc = String20.fromSlice("Microsoft Corp"),
        .security_type = String3.fromSlice("CS"),
        .last_qty = 500,
        .last_px_mantissa = Decimal64.fromFloat(420.50).mantissa,
    };

    var buf: [1024]u8 = undefined;
    const written = msg.encode(&buf);
    try testing.expect(written > 0);

    const decoded = ExecutionReport.decode(buf[0..written]);
    try testing.expectEqual(@as(u16, 56), decoded.header.template_id);
    try testing.expectEqual(@as(i64, 12345678), decoded.msg.order_id);
    try testing.expectEqual(ExecType.fill, decoded.msg.exec_type);
    try testing.expectEqual(OrdStatus.filled, decoded.msg.ord_status);
    try testing.expect(decoded.msg.symbol.eql("MSFT"));
    try testing.expectEqual(Side.sell, decoded.msg.side);
    try testing.expectEqual(@as(i32, 0), decoded.msg.leaves_qty);
    try testing.expectEqual(@as(i32, 500), decoded.msg.cum_qty);
    try testing.expectEqual(msg.avg_px_mantissa, decoded.msg.avg_px_mantissa);
}

test "MarketDataIncrementalRefresh — 3 entries encode/decode" {
    const entries = [_]MdIncGrpEntry{
        .{
            .md_update_action = .new,
            .md_price_level = 1,
            .md_entry_type = .bid,
            .security_id = 100001,
            .rpt_seq = 1,
            .md_entry_px_mantissa = Decimal64.fromFloat(150.00).mantissa,
            .number_of_orders = 42,
            .md_entry_time = 1_700_000_000_000_000_000,
            .md_entry_size = 1000,
            .aggressor_side = .buy,
            .trade_id = 0,
        },
        .{
            .md_update_action = .new,
            .md_price_level = 1,
            .md_entry_type = .offer,
            .security_id = 100001,
            .rpt_seq = 2,
            .md_entry_px_mantissa = Decimal64.fromFloat(150.05).mantissa,
            .number_of_orders = 38,
            .md_entry_time = 1_700_000_000_000_000_001,
            .md_entry_size = 800,
            .aggressor_side = .sell,
            .trade_id = 0,
        },
        .{
            .md_update_action = .change,
            .md_price_level = 2,
            .md_entry_type = .bid,
            .security_id = 100001,
            .rpt_seq = 3,
            .md_entry_px_mantissa = Decimal64.fromFloat(149.95).mantissa,
            .number_of_orders = 15,
            .md_entry_time = 1_700_000_000_000_000_002,
            .md_entry_size = 500,
            .aggressor_side = .buy,
            .trade_id = 0,
        },
    };

    var buf: [4096]u8 = undefined;
    const written = MarketDataIncrementalRefresh.encode(19876, &entries, &buf);
    try testing.expect(written > 0);

    const decoded = MarketDataIncrementalRefresh.decode(buf[0..written]);
    try testing.expectEqual(@as(u16, 88), decoded.header.template_id);
    try testing.expectEqual(@as(u16, 19876), decoded.trade_date);
    try testing.expectEqual(@as(u16, 3), decoded.num_entries);

    const e0 = decoded.entry(0);
    try testing.expectEqual(MDUpdateAction.new, e0.md_update_action);
    try testing.expectEqual(MDEntryType.bid, e0.md_entry_type);
    try testing.expectEqual(@as(u64, 100001), e0.security_id);
    try testing.expectEqual(@as(i32, 1000), e0.md_entry_size);

    const e2 = decoded.entry(2);
    try testing.expectEqual(MDUpdateAction.change, e2.md_update_action);
    try testing.expectEqual(@as(u8, 2), e2.md_price_level);
}

test "MassQuote — nested groups encode/decode" {
    const root = MassQuoteRoot{
        .quote_req_id = String23.fromSlice("REQ-001"),
        .quote_id = String10.fromSlice("Q001"),
        .mm_account = String12.fromSlice("MM-ACCT"),
    };

    const entries_set0 = [_]QuoteEntry{
        .{
            .quote_entry_id = String10.fromSlice("QE-001"),
            .symbol = String6.fromSlice("ESH4"),
            .security_desc = String20.fromSlice("E-mini S&P 500"),
            .bid_px_mantissa = Decimal64.fromFloat(5000.25).mantissa,
            .bid_size = 10,
            .offer_px_mantissa = Decimal64.fromFloat(5000.50).mantissa,
            .offer_size = 10,
            .transact_time = 1_700_000_000_000_000_000,
        },
        .{
            .quote_entry_id = String10.fromSlice("QE-002"),
            .symbol = String6.fromSlice("ESM4"),
            .security_desc = String20.fromSlice("E-mini S&P Jun"),
            .bid_px_mantissa = Decimal64.fromFloat(5010.00).mantissa,
            .bid_size = 5,
            .offer_px_mantissa = Decimal64.fromFloat(5010.75).mantissa,
            .offer_size = 5,
            .transact_time = 1_700_000_000_000_000_001,
        },
    };

    const entries_set1 = [_]QuoteEntry{
        .{
            .quote_entry_id = String10.fromSlice("QE-003"),
            .symbol = String6.fromSlice("NQH4"),
            .security_desc = String20.fromSlice("E-mini Nasdaq"),
            .bid_px_mantissa = Decimal64.fromFloat(17500.00).mantissa,
            .bid_size = 3,
            .offer_px_mantissa = Decimal64.fromFloat(17500.50).mantissa,
            .offer_size = 3,
            .transact_time = 1_700_000_000_000_000_002,
        },
    };

    const quote_sets = [_]QuoteSetWithEntries{
        .{
            .set = .{
                .quote_set_id = String3.fromSlice("ES"),
                .underlying_security_desc = String20.fromSlice("S&P 500 Index"),
            },
            .entries = &entries_set0,
        },
        .{
            .set = .{
                .quote_set_id = String3.fromSlice("NQ"),
                .underlying_security_desc = String20.fromSlice("Nasdaq 100 Index"),
            },
            .entries = &entries_set1,
        },
    };

    var buf: [8192]u8 = undefined;
    const written = MassQuote.encode(root, &quote_sets, &buf);
    try testing.expect(written > 0);

    const decoded = MassQuote.decode(buf[0..written]);
    try testing.expectEqual(@as(u16, 105), decoded.header.template_id);
    try testing.expect(decoded.root.quote_req_id.eql("REQ-001"));
    try testing.expect(decoded.root.quote_id.eql("Q001"));
    try testing.expectEqual(@as(u16, 2), decoded.num_quote_sets);

    // Set 0: 2 entries
    const set0 = decoded.quoteSet(0);
    try testing.expect(set0.set.quote_set_id.eql("ES"));
    try testing.expectEqual(@as(u16, 2), set0.num_entries);
    try testing.expect(set0.entry(0).symbol.eql("ESH4"));
    try testing.expect(set0.entry(1).symbol.eql("ESM4"));

    // Set 1: 1 entry
    const set1 = decoded.quoteSet(1);
    try testing.expect(set1.set.quote_set_id.eql("NQ"));
    try testing.expectEqual(@as(u16, 1), set1.num_entries);
    try testing.expect(set1.entry(0).symbol.eql("NQH4"));
}

test "Heartbeat — encode/decode roundtrip" {
    const msg = Heartbeat{ .timestamp = 1_700_000_000_000_000_000 };
    var buf: [64]u8 = undefined;
    const written = msg.encode(&buf);
    try testing.expect(written > 0);

    const decoded = Heartbeat.decode(buf[0..written]);
    try testing.expectEqual(@as(u16, 48), decoded.header.template_id);
    try testing.expectEqual(@as(u64, 1_700_000_000_000_000_000), decoded.msg.timestamp);
}

test "Logon — encode/decode roundtrip" {
    const msg = Logon{
        .heart_bt_int = 30,
        .username = String20.fromSlice("trader01"),
        .password = String20.fromSlice("s3cret!"),
        .reset_seq_num = .true_val,
    };
    var buf: [256]u8 = undefined;
    const written = msg.encode(&buf);
    try testing.expect(written > 0);

    const decoded = Logon.decode(buf[0..written]);
    try testing.expectEqual(@as(u16, 65), decoded.header.template_id);
    try testing.expectEqual(@as(u32, 30), decoded.msg.heart_bt_int);
    try testing.expect(decoded.msg.username.eql("trader01"));
    try testing.expect(decoded.msg.password.eql("s3cret!"));
    try testing.expectEqual(BooleanType.true_val, decoded.msg.reset_seq_num);
}

test "Logout — encode/decode roundtrip" {
    const msg = Logout{
        .session_status = 4,
        .text = String20.fromSlice("Session ended"),
    };
    var buf: [256]u8 = undefined;
    const written = msg.encode(&buf);
    try testing.expect(written > 0);

    const decoded = Logout.decode(buf[0..written]);
    try testing.expectEqual(@as(u16, 53), decoded.header.template_id);
    try testing.expectEqual(@as(u8, 4), decoded.msg.session_status);
    try testing.expect(decoded.msg.text.eql("Session ended"));
}
