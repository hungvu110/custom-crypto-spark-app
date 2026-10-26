package vai.lakehouse.dakmock.deid;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import vai.lakehouse.dakmock.deid.DeidRequest.State;
import vai.lakehouse.dakmock.web.DakApiException;

/**
 * Lọc và phân trang danh sách phiếu, dùng chung cho danh sách của team (portal) và danh sách của admin: mới nhất trước, lọc theo
 * {@code state}, phân trang bằng {@code limit} và {@code cursor} (mã phiếu cuối trang trước, vẫn dùng được khi phiếu đó không còn
 * khớp bộ lọc). Tham số là giá trị thô của query string; {@code null} nghĩa là không khai.
 */
final class RequestListing {

  private static final int DEFAULT_LIMIT = 20;
  private static final int MAX_LIMIT = 100;

  private RequestListing() {}

  /** {@code newestFirst} là TOÀN BỘ phiếu người gọi được phép thấy, mới nhất trước. */
  static DeidRequestService.Page page(List<DeidRequest> newestFirst, String state, String limit, String cursor) {
    State filter = parseState(state);
    int max = parseLimit(limit);
    int from = cursor == null ? 0 : indexAfter(newestFirst, cursor);
    List<DeidRequest> remaining = newestFirst.subList(from, newestFirst.size()).stream()
        .filter(r -> filter == null || r.state() == filter)
        .toList();
    if (remaining.size() <= max) {
      return new DeidRequestService.Page(remaining, null);
    }
    List<DeidRequest> page = remaining.subList(0, max);
    return new DeidRequestService.Page(List.copyOf(page), page.get(max - 1).requestCode());
  }

  private static int indexAfter(List<DeidRequest> all, String cursor) {
    if (DeidRequest.isCode(cursor)) {
      for (int i = 0; i < all.size(); i++) {
        if (all.get(i).requestCode().equals(cursor)) {
          return i + 1;
        }
      }
    }
    // Cursor lạ, sai định dạng hay của team khác dùng cùng một lỗi: không lộ phiếu có tồn tại.
    throw DakApiException.invalidRequest("invalid cursor");
  }

  private static State parseState(String state) {
    if (state == null) {
      return null;
    }
    try {
      return State.valueOf(state.toUpperCase(Locale.ROOT));
    } catch (IllegalArgumentException e) {
      String valid = Arrays.stream(State.values()).map(Enum::name).collect(Collectors.joining(", "));
      throw DakApiException.invalidRequest("state must be one of " + valid);
    }
  }

  private static int parseLimit(String limit) {
    if (limit == null) {
      return DEFAULT_LIMIT;
    }
    try {
      int value = Integer.parseInt(limit);
      if (value >= 1 && value <= MAX_LIMIT) {
        return value;
      }
    } catch (NumberFormatException e) {
      // rơi xuống lỗi chung bên dưới
    }
    throw DakApiException.invalidRequest("limit must be an integer between 1 and " + MAX_LIMIT);
  }
}
