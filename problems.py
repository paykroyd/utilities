def char_ordered_overlap(a, b):
    """
    Given strings a and b, return a string with the intersection of characters, ordered by a.
    """
    common = set(a).intersection(set(b))
    result = ''
    for ch in a:
        if ch in common:
            result += ch
            common.remove(ch)
    return result


def combinations(amount):
    """
    Returns different combinations of coins to make change for the amount.
    """
    def remaining(quarters=0, dimes=0, nickels=0):
        return round(amount - (quarters * .25) - (dimes * .1) - (nickels * .05), 2)

    results = []
    for quarters in range(0, int(1 + amount / .25)):
        for dimes in range(0, int(1 + remaining(quarters) / .1)):
            for nickels in range(0, int(1 + remaining(quarters, dimes) / .05)):
                results.append({'q': quarters,
                                'd': dimes,
                                'n': nickels,
                                'p': int(remaining(quarters, dimes, nickels) * 100)})
    return results


def brackets_match(expression):
    """
    Ensures that a string composed of (, ), {, }, [, ] is valid. All opens are closed.
    """
    OPENERS = ('(', '{', '[')
    CLOSERS = (')', '}', ']')
    stack = []
    for ch in expression:
        if ch in OPENERS:
            stack.append(ch)
        elif ch in CLOSERS:
            if CLOSERS.index(ch) != OPENERS.index(stack.pop()):
                return False
        else:
            raise ValueError('"%s" is not a valid character' % ch)
    return len(stack) == 0


def shortest_path(start, end):
    """
    Shortest path between two points.
    """
    path = [start]
    x = start[0]
    while x > end[0]:
        x -= 1
        path.append((x, start[1]))
    y = start[1]
    while y < end[1]:
        y += 1
        path.append((x, y))
    return path


def biggest_matrix(m):
    """
    Given a matrix m with positive and negative numbers. Find the largest summing sub matrix.
    """
    def submatrix(i, j, rows, cols):
        """
        gets the sub matrix starting at ij of size rows x cols.
        """
        sub = []
        for x in range(i, i + rows):
            row = []
            sub.append(row)
            for y in range(j, j + cols):
                row.append(m[x][y])
        return sub

    def matrix_sum(matrix):
        """
        sums the matrix
        """
        return sum([sum(row) for row in matrix])

    largest = None
    lsum = 0
    for i in range(0, len(m)):
        for j in range(0, len(m[0])):
            # iterate through the different sizes
            for rows in range(1, len(m) - i + 1):
                for cols in range(1, len(m[0]) - j + 1):
                    sub = submatrix(i, j, rows, cols)
                    ssum = matrix_sum(sub)
                    if largest is None or ssum > lsum:
                        largest = sub
                        lsum = ssum
    return largest


def find_start_of_circular_list(l):
    """
    Given a circular, ordered list (e.g, 7, 8, 9, 0, 1, 4, 5). Find the start index.
    """
    # 7 8 2 3 4 4 5 6 6 7
    #         ^
    def f(pivot, start, end):
        p = l[pivot]

        # try to figure out if it's this is the start
        if l[pivot] < l[pivot - 1]:
            return pivot

        if pivot != start:
            left_pivot = start + ((pivot - start) / 2)
        else:
            left_pivot = None

        if pivot + 1 != end:
            right_pivot = pivot + ((end - pivot) / 2)
        else:
            right_pivot = None

        if not(right_pivot is not None and l[right_pivot] < p) \
           and left_pivot is not None:
            val = f(left_pivot, start, pivot)
            if val is not None:
                return val

        if right_pivot is not None:
            return f(right_pivot, pivot + 1, end)

        return None

    return f(len(l) / 2, 0, len(l))
