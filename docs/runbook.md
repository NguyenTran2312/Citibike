## ✅ Definition of Done (DoD)

Một task được xem là **hoàn thành** khi đáp ứng các tiêu chí sau:

- [ ] **Working Code:** Code chạy đúng và không có lỗi nghiêm trọng.
- [ ] **Reproducibility:** Có hướng dẫn để chạy lại và tái hiện kết quả.
- [ ] **Testing:** Có test cho các trường hợp chính (happy path và edge cases quan trọng).
- [ ] **Logging:** Log lỗi cung cấp đủ thông tin để debug.

### Testing Requirements

Không bắt buộc viết unit test cho từng hàm. Tuy nhiên, các thành phần sau **phải có test**:

- **Parser:** Kiểm tra dữ liệu hợp lệ, không hợp lệ và dữ liệu thiếu.
- **Timestamp:** Kiểm tra timezone, timestamp parsing và các trường hợp biên.
- **Deduplication:** Kiểm tra dữ liệu trùng lặp và tính idempotency.
- **State Logic:** Kiểm tra chuyển đổi trạng thái và xử lý dữ liệu đến sai thứ tự.
