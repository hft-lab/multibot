import traceback


class Shifts:

    def __init__(self):
        with open('rates.txt', 'r') as file:
            data = file.read().split('\n\n')

        self.data = data[-300000:]
        self.list_len = len(data)
        self.price_deviations = []
        self.prices = {}

    def find_deviations(self):
        for record in self.data:
            for exchange in record.split('\n'):
                if exchange == '':
                    continue
                name = exchange.split(' | ')[0]
                price = float(exchange.split(' | ')[1])
                self.prices.update({name: price})

            # avr_price = sum([x for x in self.prices.values()]) / len(self.prices.values())
            new_record = {}
            for name_1 in self.prices.keys():
                for name_2 in self.prices.keys():
                    if name_2 == name_1:
                        continue
                    # if new_record.get(name_2 + ' ' + name_1):
                    #     continue
                    deviation_value = (self.prices[name_1] - self.prices[name_2]) / self.prices[name_2]
                    # print(f'{exchange}: {deviation_value}')
                    new_record.update({name_1 + ' ' + name_2: deviation_value})
            self.price_deviations.append(new_record)

    def get_shifts(self):
        self.find_deviations()
        gathering_time = self.list_len / 240
        shifts = {}
        print(f"Result for {gathering_time} hours")

        for exchange in self.price_deviations[-1].keys():
            try:
                avg_deviation = sum([x[exchange] for x in self.price_deviations]) / self.list_len
            except Exception as e:
                traceback.print_exc()
                avg_deviation = None

            shifts.update({exchange: round(avg_deviation, 6)})

        print(f"SHIFTS: {shifts}")

        return shifts


