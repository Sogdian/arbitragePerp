from one_coin_bot import _parse_summary_from_line


def main() -> None:
    line = (
        "📈 Long gate Ц/Ф: 17.615 / -1.659% | 📉 Short bybit Ц/Ф: 17.833 / -0.244% | "
        "📊 Спред Ц/Ф/О: 1.163% / 1.415% / 2.578% | ✅ арбитр (gate: 2.271 RIVER, bybit: 2.243 RIVER)"
    )
    print("LINE:", line)
    parsed = _parse_summary_from_line(line)
    print("PARSED:", parsed)


if __name__ == "__main__":
    main()

