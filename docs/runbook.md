# Runbook — Quy trình làm việc và vận hành

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

Test thủ công chỉ được tính khi script, câu SQL hoặc các bước chạy lại được commit vào `tests/` hoặc README của module.

## 🔁 Quy trình issue và PR

1. **Mở issue:** ghi mục `docs/scope.md` / `docs/contracts.md` mà issue đáp ứng và danh sách **tiêu chí hoàn thành** kiểm tra được. Người phụ trách theo `docs/contracts.md` mục 4.
2. **Thêm việc mới vào kế hoạch:** cập nhật bảng truy vết `docs/contracts.md` mục 4.1; nếu scope có yêu cầu chưa có issue, ghi **Chưa có issue** thay vì bỏ qua.
3. **Đổi chuẩn** (schema, grain, quy tắc, cấu hình dùng chung): sửa `docs/contracts.md` trong **cùng PR** với code/test. Chốt một mục Pending thì cập nhật mục 5.
4. **README module** chỉ mô tả cách chạy; quy tắc dữ liệu thì link về mục contract tương ứng.
5. **Đóng issue** chỉ khi PR liên quan đã được review và merge vào `main`, và các tiêu chí hoàn thành đều đạt.

## 🛠️ Vận hành

| Việc | Tài liệu |
|---|---|
| Khởi động, dừng, xử lý sự cố stack Docker Compose | [infra/README.md](../infra/README.md) |
| Smoke test và các loại test | [tests/README.md](../tests/README.md) |
| Chạy từng module | `README.md` trong thư mục module (`ingestion/`, `batch/`, `streaming/`, `models/`) |
