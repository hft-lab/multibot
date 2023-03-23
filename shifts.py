
class Shifts:

    with open('rates.txt', 'r') as file:
        data = file.read().split('\n\n')
    data = data[-300000:]
    list_len = len(data)
    price_deviations = []
    prices = {}

    def find_deviations(self):
        for record in self.data:
            # print(record + '\n')
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

    # def check_for_repeats(self, shifts):
    #     for key_1 in shifts.keys():
    #         for key_2

    def get_shifts(self):
        self.find_deviations()
        gathering_time = len(self.data) / 240
        # print(f"Gathering time: {gathering_time}")
        shifts = {}
        # if gathering_time > 10:
        print(f"Result for {gathering_time} hours")
        for exchange in self.price_deviations[-1].keys():
            try:
                avg_deviation = sum([x[exchange] for x in self.price_deviations]) / self.list_len
            except Exception as e:
                print(f"Line 39. Shifts. Error: {e}")
                avg_deviation = None
            # print(f"Avg deviation {exchange}: {avg_deviation}")
            shifts.update({exchange: round(avg_deviation, 6)})
        # shifts = self.check_for_repeats(shifts)
        print(f"SHIFTS: {shifts}")

        return shifts


# shifts = Shifts()
# shifts.get_shifts()


