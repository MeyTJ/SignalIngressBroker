package registry

type session struct {
	outbound chan []byte
	onClose  func()
}
