// Generated by go-rpcgen. Do not modify.

package maggiefs

import (
	"net/rpc"
)

type PeerService struct {
	impl Peer
}

func NewPeerService(impl Peer) *PeerService {
	return &PeerService{impl}
}

func RegisterPeerService(impl Peer) error {
	return rpc.RegisterName("Peer", NewPeerService(impl))
}

type PeerHeartBeatRequest struct {
}

type PeerHeartBeatResponse struct {
	Stat *DataNodeStat
}

func (s *PeerService) HeartBeat(request *PeerHeartBeatRequest, response *PeerHeartBeatResponse) (err error) {
	response.Stat, err = s.impl.HeartBeat()
	return
}

type PeerAddBlockRequest struct {
	Blk       Block
	VolId     uint32
	Fallocate bool
}

type PeerAddBlockResponse struct {
}

func (s *PeerService) AddBlock(request *PeerAddBlockRequest, response *PeerAddBlockResponse) (err error) {
	err = s.impl.AddBlock(request.Blk, request.VolId, request.Fallocate)
	return
}

type PeerRmBlockRequest struct {
	Id    uint64
	VolId uint32
}

type PeerRmBlockResponse struct {
}

func (s *PeerService) RmBlock(request *PeerRmBlockRequest, response *PeerRmBlockResponse) (err error) {
	err = s.impl.RmBlock(request.Id, request.VolId)
	return
}

type PeerTruncBlockRequest struct {
	Blk     Block
	VolId   uint32
	NewSize uint32
}

type PeerTruncBlockResponse struct {
}

func (s *PeerService) TruncBlock(request *PeerTruncBlockRequest, response *PeerTruncBlockResponse) (err error) {
	err = s.impl.TruncBlock(request.Blk, request.VolId, request.NewSize)
	return
}

type PeerBlockReportRequest struct {
	VolId uint32
}

type PeerBlockReportResponse struct {
	Blocks []Block
}

func (s *PeerService) BlockReport(request *PeerBlockReportRequest, response *PeerBlockReportResponse) (err error) {
	response.Blocks, err = s.impl.BlockReport(request.VolId)
	return
}

type PeerClient struct {
	client  *rpc.Client
	service string
}

func NewPeerClient(client *rpc.Client) *PeerClient {
	return &PeerClient{client, "Peer"}
}

func (_c *PeerClient) HeartBeat() (stat *DataNodeStat, err error) {
	_request := &PeerHeartBeatRequest{}
	_response := &PeerHeartBeatResponse{}
	err = _c.client.Call(_c.service+".HeartBeat", _request, _response)
	return _response.Stat, err
}

func (_c *PeerClient) AddBlock(blk Block, volId uint32, fallocate bool) (err error) {
	_request := &PeerAddBlockRequest{blk, volId, fallocate}
	_response := &PeerAddBlockResponse{}
	err = _c.client.Call(_c.service+".AddBlock", _request, _response)
	return err
}

func (_c *PeerClient) RmBlock(id uint64, volId uint32) (err error) {
	_request := &PeerRmBlockRequest{id, volId}
	_response := &PeerRmBlockResponse{}
	err = _c.client.Call(_c.service+".RmBlock", _request, _response)
	return err
}

func (_c *PeerClient) TruncBlock(blk Block, volId uint32, newSize uint32) (err error) {
	_request := &PeerTruncBlockRequest{blk, volId, newSize}
	_response := &PeerTruncBlockResponse{}
	err = _c.client.Call(_c.service+".TruncBlock", _request, _response)
	return err
}

func (_c *PeerClient) BlockReport(volId uint32) (blocks []Block, err error) {
	_request := &PeerBlockReportRequest{volId}
	_response := &PeerBlockReportResponse{}
	err = _c.client.Call(_c.service+".BlockReport", _request, _response)
	return _response.Blocks, err
}
