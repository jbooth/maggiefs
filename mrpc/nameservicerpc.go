package mrpc

import (
	"github.com/jbooth/maggiefs/maggiefs"
	"net/rpc"
)

type NameServiceService struct {
	impl maggiefs.NameService
}

func NewNameServiceService(impl maggiefs.NameService) *NameServiceService {
	return &NameServiceService{impl}
}

func RegisterNameServiceService(impl maggiefs.NameService) error {
	return rpc.RegisterName("NameService", NewNameServiceService(impl))
}

type NameServiceGetInodeRequest struct {
	Nodeid uint64
}

type NameServiceGetInodeResponse struct {
	I *maggiefs.Inode
}

func (s *NameServiceService) GetInode(request *NameServiceGetInodeRequest, response *NameServiceGetInodeResponse) (err error) {
	response.I, err = s.impl.GetInode(request.Nodeid)
	return
}

type NameServiceStatFsRequest struct {
}

type NameServiceStatFsResponse struct {
	Stat maggiefs.FsStat
}

func (s *NameServiceService) StatFs(request *NameServiceStatFsRequest, response *NameServiceStatFsResponse) (err error) {
	response.Stat, err = s.impl.StatFs()
	return
}

type NameServiceAddInodeRequest struct {
	Node *maggiefs.Inode
}

type NameServiceAddInodeResponse struct {
	Id uint64
}

func (s *NameServiceService) AddInode(request *NameServiceAddInodeRequest, response *NameServiceAddInodeResponse) (err error) {
	response.Id, err = s.impl.AddInode(request.Node)
	return
}

type NameServiceSetAttrRequest struct {
	Nodeid uint64
	Arg    maggiefs.SetAttr
}

type NameServiceSetAttrResponse struct {
	NewNode *maggiefs.Inode
}

func (s *NameServiceService) SetAttr(request *NameServiceSetAttrRequest, response *NameServiceSetAttrResponse) (err error) {
	response.NewNode, err = s.impl.SetAttr(request.Nodeid, request.Arg)
	return
}

type NameServiceSetXAttrRequest struct {
	Nodeid uint64
	Name   []byte
	Val    []byte
}

type NameServiceSetXAttrResponse struct {
}

func (s *NameServiceService) SetXAttr(request *NameServiceSetXAttrRequest, response *NameServiceSetXAttrResponse) (err error) {
	err = s.impl.SetXAttr(request.Nodeid, request.Name, request.Val)
	return
}

type NameServiceDelXAttrRequest struct {
	Nodeid uint64
	Name   []byte
}

type NameServiceDelXAttrResponse struct {
}

func (s *NameServiceService) DelXAttr(request *NameServiceDelXAttrRequest, response *NameServiceDelXAttrResponse) (err error) {
	err = s.impl.DelXAttr(request.Nodeid, request.Name)
	return
}

type NameServiceExtendRequest struct {
	Nodeid uint64
	NewLen uint64
}

type NameServiceExtendResponse struct {
	NewNode *maggiefs.Inode
}

func (s *NameServiceService) Extend(request *NameServiceExtendRequest, response *NameServiceExtendResponse) (err error) {
	response.NewNode, err = s.impl.Extend(request.Nodeid, request.NewLen)
	return
}

type NameServiceTruncateRequest struct {
	Nodeid uint64
	NewLen uint64
}

type NameServiceTruncateResponse struct {
	NewNode *maggiefs.Inode
}

func (s *NameServiceService) Truncate(request *NameServiceTruncateRequest, response *NameServiceTruncateResponse) (err error) {
	response.NewNode, err = s.impl.Truncate(request.Nodeid, request.NewLen)
	return
}

type NameServiceAddBlockRequest struct {
	Nodeid        uint64
	BlockStartPos uint64
	RequestedDnId *uint32
}

type NameServiceAddBlockResponse struct {
	NewNode *maggiefs.Inode
}

func (s *NameServiceService) AddBlock(request *NameServiceAddBlockRequest, response *NameServiceAddBlockResponse) (err error) {
	response.NewNode, err = s.impl.AddBlock(request.Nodeid, request.BlockStartPos, request.RequestedDnId)
	return
}

type NameServiceFallocateRequest struct {
	Nodeid        uint64
	Length        uint64
	RequestedDnId *uint32
}

type NameServiceFallocateResponse struct {
}

func (s *NameServiceService) Fallocate(request *NameServiceFallocateRequest, response *NameServiceFallocateResponse) (err error) {
	err = s.impl.Fallocate(request.Nodeid, request.Length, request.RequestedDnId)
	return
}

type NameServiceLinkRequest struct {
	Parent uint64
	Child  uint64
	Name   string
	Force  bool
}

type NameServiceLinkResponse struct {
}

func (s *NameServiceService) Link(request *NameServiceLinkRequest, response *NameServiceLinkResponse) (err error) {
	err = s.impl.Link(request.Parent, request.Child, request.Name, request.Force)
	return
}

type NameServiceUnlinkRequest struct {
	Parent uint64
	Name   string
}

type NameServiceUnlinkResponse struct {
}

func (s *NameServiceService) Unlink(request *NameServiceUnlinkRequest, response *NameServiceUnlinkResponse) (err error) {
	err = s.impl.Unlink(request.Parent, request.Name)
	return
}

type NameServiceJoinRequest struct {
	DnId         uint32
	NameDataAddr string
}

type NameServiceJoinResponse struct {
}

func (s *NameServiceService) Join(request *NameServiceJoinRequest, response *NameServiceJoinResponse) (err error) {
	err = s.impl.Join(request.DnId, request.NameDataAddr)
	return
}

type NameServiceNextVolIdRequest struct {
}

type NameServiceNextVolIdResponse struct {
	Id uint32
}

func (s *NameServiceService) NextVolId(request *NameServiceNextVolIdRequest, response *NameServiceNextVolIdResponse) (err error) {
	response.Id, err = s.impl.NextVolId()
	return
}

type NameServiceNextDnIdRequest struct {
}

type NameServiceNextDnIdResponse struct {
	Id uint32
}

func (s *NameServiceService) NextDnId(request *NameServiceNextDnIdRequest, response *NameServiceNextDnIdResponse) (err error) {
	response.Id, err = s.impl.NextDnId()
	return
}

type NameServiceClient struct {
	client  *rpc.Client
	service string
}

func NewNameServiceClient(client *rpc.Client) *NameServiceClient {
	return &NameServiceClient{client, "NameService"}
}

func (_c *NameServiceClient) GetInode(nodeid uint64) (i *maggiefs.Inode, err error) {
	_request := &NameServiceGetInodeRequest{nodeid}
	_response := &NameServiceGetInodeResponse{}
	err = _c.client.Call(_c.service+".GetInode", _request, _response)
	return _response.I, err
}

func (_c *NameServiceClient) StatFs() (stat maggiefs.FsStat, err error) {
	_request := &NameServiceStatFsRequest{}
	_response := &NameServiceStatFsResponse{}
	err = _c.client.Call(_c.service+".StatFs", _request, _response)
	return _response.Stat, err
}

func (_c *NameServiceClient) AddInode(node *maggiefs.Inode) (id uint64, err error) {
	_request := &NameServiceAddInodeRequest{node}
	_response := &NameServiceAddInodeResponse{}
	err = _c.client.Call(_c.service+".AddInode", _request, _response)
	return _response.Id, err
}

func (_c *NameServiceClient) SetAttr(nodeid uint64, arg maggiefs.SetAttr) (newNode *maggiefs.Inode, err error) {
	_request := &NameServiceSetAttrRequest{nodeid, arg}
	_response := &NameServiceSetAttrResponse{}
	err = _c.client.Call(_c.service+".SetAttr", _request, _response)
	return _response.NewNode, err
}

func (_c *NameServiceClient) SetXAttr(nodeid uint64, name []byte, val []byte) (err error) {
	_request := &NameServiceSetXAttrRequest{nodeid, name, val}
	_response := &NameServiceSetXAttrResponse{}
	err = _c.client.Call(_c.service+".SetXAttr", _request, _response)
	return err
}

func (_c *NameServiceClient) DelXAttr(nodeid uint64, name []byte) (err error) {
	_request := &NameServiceDelXAttrRequest{nodeid, name}
	_response := &NameServiceDelXAttrResponse{}
	err = _c.client.Call(_c.service+".DelXAttr", _request, _response)
	return err
}

func (_c *NameServiceClient) Extend(nodeid uint64, newLen uint64) (newNode *maggiefs.Inode, err error) {
	_request := &NameServiceExtendRequest{nodeid, newLen}
	_response := &NameServiceExtendResponse{}
	err = _c.client.Call(_c.service+".Extend", _request, _response)
	return _response.NewNode, err
}

func (_c *NameServiceClient) Truncate(nodeid uint64, newLen uint64) (newNode *maggiefs.Inode, err error) {
	_request := &NameServiceTruncateRequest{nodeid, newLen}
	_response := &NameServiceTruncateResponse{}
	err = _c.client.Call(_c.service+".Truncate", _request, _response)
	return _response.NewNode, err
}

func (_c *NameServiceClient) AddBlock(nodeid uint64, blockStartPos uint64, requestedDnId *uint32) (newNode *maggiefs.Inode, err error) {
	_request := &NameServiceAddBlockRequest{nodeid, blockStartPos, requestedDnId}
	_response := &NameServiceAddBlockResponse{}
	err = _c.client.Call(_c.service+".AddBlock", _request, _response)
	return _response.NewNode, err
}

func (_c *NameServiceClient) Fallocate(nodeid uint64, length uint64, requestedDnId *uint32) (err error) {
	_request := &NameServiceFallocateRequest{nodeid, length, requestedDnId}
	_response := &NameServiceFallocateResponse{}
	err = _c.client.Call(_c.service+".Fallocate", _request, _response)
	return err
}

func (_c *NameServiceClient) Link(parent uint64, child uint64, name string, force bool) (err error) {
	_request := &NameServiceLinkRequest{parent, child, name, force}
	_response := &NameServiceLinkResponse{}
	err = _c.client.Call(_c.service+".Link", _request, _response)
	return err
}

func (_c *NameServiceClient) Unlink(parent uint64, name string) (err error) {
	_request := &NameServiceUnlinkRequest{parent, name}
	_response := &NameServiceUnlinkResponse{}
	err = _c.client.Call(_c.service+".Unlink", _request, _response)
	return err
}

func (_c *NameServiceClient) Join(dnId uint32, nameDataAddr string) (err error) {
	_request := &NameServiceJoinRequest{dnId, nameDataAddr}
	_response := &NameServiceJoinResponse{}
	err = _c.client.Call(_c.service+".Join", _request, _response)
	return err
}

func (_c *NameServiceClient) NextVolId() (id uint32, err error) {
	_request := &NameServiceNextVolIdRequest{}
	_response := &NameServiceNextVolIdResponse{}
	err = _c.client.Call(_c.service+".NextVolId", _request, _response)
	return _response.Id, err
}

func (_c *NameServiceClient) NextDnId() (id uint32, err error) {
	_request := &NameServiceNextDnIdRequest{}
	_response := &NameServiceNextDnIdResponse{}
	err = _c.client.Call(_c.service+".NextDnId", _request, _response)
	return _response.Id, err
}
