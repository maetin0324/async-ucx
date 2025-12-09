use super::*;
use ucx1_sys::{ucp_op_attr_t, ucp_request_param_t};

impl Endpoint {
    /// Sends data through stream.
    #[async_backtrace::framed]
    pub async fn stream_send(&self, buf: &[u8]) -> Result<usize, Error> {
        trace!("stream_send: endpoint={:?} len={}", self.handle, buf.len());
        unsafe extern "C" fn callback(
            request: *mut c_void,
            status: ucs_status_t,
            _user_data: *mut c_void,
        ) {
            trace!(
                "stream_send: complete. req={:?}, status={:?}",
                request,
                status
            );
            let request = &mut *(request as *mut Request);
            request.waker.wake();
        }
        let param = ucp_request_param_t {
            op_attr_mask: ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK as u32,
            cb: ucx1_sys::ucp_request_param_t__bindgen_ty_1 {
                send: Some(callback),
            },
            ..unsafe { MaybeUninit::zeroed().assume_init() }
        };
        let status = unsafe {
            ucp_stream_send_nbx(
                self.get_handle()?,
                buf.as_ptr() as _,
                buf.len() as _,
                &param,
            )
        };
        if status.is_null() {
            trace!("stream_send: complete");
        } else if UCS_PTR_IS_PTR(status) {
            request_handle(status, poll_normal).await?;
        } else {
            return Err(Error::from_ptr(status).unwrap_err());
        }
        Ok(buf.len())
    }

    /// Receives data from stream.
    #[async_backtrace::framed]
    pub async fn stream_recv(&self, buf: &mut [MaybeUninit<u8>]) -> Result<usize, Error> {
        trace!("stream_recv: endpoint={:?} len={}", self.handle, buf.len());
        unsafe extern "C" fn callback(
            request: *mut c_void,
            status: ucs_status_t,
            length: usize,
            _user_data: *mut c_void,
        ) {
            trace!(
                "stream_recv: complete. req={:?}, status={:?}, len={}",
                request,
                status,
                length
            );
            let request = &mut *(request as *mut Request);
            request.waker.wake();
        }
        let mut length = MaybeUninit::<usize>::uninit();
        let param = ucp_request_param_t {
            op_attr_mask: ucp_op_attr_t::UCP_OP_ATTR_FIELD_CALLBACK as u32,
            cb: ucx1_sys::ucp_request_param_t__bindgen_ty_1 {
                recv_stream: Some(callback),
            },
            ..unsafe { MaybeUninit::zeroed().assume_init() }
        };
        let status = unsafe {
            ucp_stream_recv_nbx(
                self.get_handle()?,
                buf.as_mut_ptr() as _,
                buf.len() as _,
                length.as_mut_ptr(),
                &param,
            )
        };
        if status.is_null() {
            let length = unsafe { length.assume_init() } as usize;
            trace!("stream_recv: complete. len={}", length);
            Ok(length)
        } else if UCS_PTR_IS_PTR(status) {
            Ok(request_handle(status, poll_stream).await)
        } else {
            Err(Error::from_ptr(status).unwrap_err())
        }
    }
}

unsafe fn poll_stream(ptr: ucs_status_ptr_t) -> Poll<usize> {
    let mut len = MaybeUninit::<usize>::uninit();
    let status = ucp_stream_recv_request_test(ptr as _, len.as_mut_ptr() as _);
    if status == ucs_status_t::UCS_INPROGRESS {
        Poll::Pending
    } else {
        Poll::Ready(len.assume_init())
    }
}
