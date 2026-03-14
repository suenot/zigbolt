# ZigBolt Security Audit

Perform security assessment for ZigBolt (shared memory IPC + network messaging).

## Instructions

1. **Shared Memory Security**
   - Review shm_open permissions (should be 0600, not world-readable)
   - Check for TOCTOU races in shared memory setup
   - Verify shm_unlink cleanup on exit/error
   - Review mmap protection flags
   - Check for information leakage in shared memory regions

2. **Network Security**
   - Review UDP socket configuration (bind address restrictions)
   - Check for buffer overflow in recv paths
   - Verify packet size validation before processing
   - Review multicast group security
   - Check for amplification attack vectors

3. **Buffer Safety**
   - Verify all buffer accesses are bounds-checked
   - Check for integer overflow in size calculations
   - Review @ptrCast/@alignCast safety
   - Verify frame_length validation before buffer access
   - Check for stack buffer overflow in packet construction

4. **Input Validation**
   - Verify all network packet headers are validated
   - Check sequence number overflow handling
   - Review NAK message validation
   - Verify fragment reassembly bounds

5. **Resource Management**
   - Check for file descriptor leaks
   - Verify memory map cleanup in all error paths
   - Review allocator usage and leak potential
   - Check for unbounded allocation (DoS vector)

6. **FFI Safety**
   - Review C ABI exports for null pointer handling
   - Check pointer validity in exported functions
   - Verify lifetime management across FFI boundary

7. **Report**
   - Severity classification (Critical/High/Medium/Low)
   - Specific file:line references
   - Remediation recommendations with code
