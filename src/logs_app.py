import click
from datetime import datetime
from .generator import generate_all_data
from .loader import load_all_data
from .aggregator import aggregate_and_export
from .config import Config


@click.group()
def main():
    """Тестовое задание для FarPost: генерация, загрузка и агрегация логов форума"""
    pass


@main.command()
@click.option("--start-date", default=Config.DEFAULT_START_DATE.strftime("%Y-%m-%d"), help="Дата начала (YYYY-MM-DD)")
@click.option("--days", default=30, help="Количество дней для генерации")
@click.option("--users", default=100, help="Количество пользователей")
def generate(start_date, days, users):
    """Генерация CSV файлов с данными для пользователей, тем, сообщений и логов"""
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        output = Config.DATA_DIR

        click.echo(f"📊 Генерация данных от {start.date()} на {days} дней...")
        stats = generate_all_data(
            start_date=start, days=days, n_users=users, output_dir=output
        )

        click.echo(click.style("✅ Генерация завершена!", fg="green"))
        click.echo(f"   Пользователи: {stats['users']}")
        click.echo(f"   Темы: {stats['topics']}")
        click.echo(f"   Сообщения: {stats['messages']}")
        click.echo(f"   Логи: {stats['logs']}")

    except Exception as e:
        click.echo(click.style(f"❌ Ошибка: {e}", fg="red"))
        raise SystemExit(1)


@main.command()
@click.option(
    "--truncate-tables", is_flag=True, help="Очистить существующие таблицы перед загрузкой данных"
)
@click.pass_context
def load(ctx, truncate_tables):
    """Загрузка данных из CSV в PostgreSQL"""
    try:
        click.echo("📥 Загрузка данных в базу данных...")
        stats = load_all_data(truncate_tables=truncate_tables)

        click.echo(click.style("✅ Загрузка завершена!", fg="green"))
        for table, count in stats.items():
            click.echo(f"   {table}: {count} rows")

    except Exception as e:
        click.echo(click.style(f"❌ Ошибка: {e}", fg="red"))
        raise SystemExit(1)


@main.command()
@click.option("--from-date", required=True, help="Дата начала (YYYY-MM-DD)")
@click.option("--to-date", required=True, help="Дата окончания (YYYY-MM-DD)")
@click.option("--output", default="report.csv", help="Выходной файл CSV для отчета")
def select(from_date, to_date, output):
    """Агрегация данных за указанный период и экспорт в CSV"""
    try:
        start = datetime.strptime(from_date, "%Y-%m-%d")
        end = datetime.strptime(to_date, "%Y-%m-%d")

        if start > end:
            raise ValueError("from-date must be before to-date")

        click.echo(f"📈 Агрегация данных от {start.date()} до {end.date()}...")
        rows = aggregate_and_export(start_date=start, end_date=end, output_file=output)

        click.echo(click.style("✅ Агрегация завершена!", fg="green"))
        click.echo(f"   Файл: {output}")
        click.echo(f"   Строки: {len(rows)}")

    except Exception as e:
        click.echo(click.style(f"❌ Ошибка: {e}", fg="red"))
        raise SystemExit(1)


@main.command()
@click.pass_context
def full(ctx):
    """Выполнить полный пайплайн: generate → load → select (последние 30 дней)"""
    ctx.invoke(generate)
    ctx.invoke(load)

    from datetime import timedelta

    end = Config.DEFAULT_START_DATE + timedelta(days=Config.DEFAULT_DAYS)
    ctx.invoke(
        select,
        from_date=Config.DEFAULT_START_DATE.strftime("%Y-%m-%d"),
        to_date=end.strftime("%Y-%m-%d"),
        output="report.csv",
    )


if __name__ == "__main__":
    main()
