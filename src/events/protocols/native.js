/**
 * Protocol: `native` — lossless passthrough of internal gateway frames.
 *
 * The only format that carries every internal frame (tools, approvals,
 * status, phase). All other protocols are documented projections.
 */
export default {
  id: 'native',
  title: 'Native foreman frames',
  description: 'Lossless passthrough: internal frames are forwarded unchanged.',
  create() {
    return {
      push(frame) {
        return [{ type: 'data', payload: frame }]
      },
    }
  },
}
